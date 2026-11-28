//! Normalization and pagination rules for 7615 news feeds.

use std::collections::HashSet;

use eltdx_store::NewsArticleInput;
use serde_json::{json, Map, Value};

pub const NEWS_SOURCE: &str = "tdx";

/// Home tabs exposed by 7615. Tab 8 is disabled in the upstream client.
pub const HOME_TABS: [i64; 8] = [1, 2, 3, 4, 5, 6, 7, 9];

/// Build the market-news home scopes used by the default schedule.
pub fn home_scopes() -> Vec<NewsScope> {
    HOME_TABS
        .iter()
        .map(|tab| NewsScope::Home { tab: *tab })
        .collect()
}

/// Build channel scopes from the enabled channel catalog.
pub fn channel_scopes(channel_ids: impl IntoIterator<Item = i64>) -> Vec<NewsScope> {
    channel_ids
        .into_iter()
        .map(|channel_id| NewsScope::Channel { channel_id })
        .collect()
}

/// One logical news feed: a home tab, a numbered channel, or one company kind.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum NewsScope {
    Home { tab: i64 },
    Channel { channel_id: i64 },
    Company { code: String, kind: String },
}

impl NewsScope {
    pub fn dataset(&self) -> &'static str {
        match self {
            Self::Home { .. } | Self::Channel { .. } => "news.market",
            Self::Company { kind, .. } => match normalized_company_kind(kind) {
                "announcement" => "news.announcement",
                "roadshow" => "news.roadshow",
                _ => "news.company",
            },
        }
    }

    pub fn scope_type(&self) -> &'static str {
        match self {
            Self::Home { .. } => "home",
            Self::Channel { .. } => "channel",
            Self::Company { .. } => "company",
        }
    }

    pub fn scope_key(&self) -> String {
        match self {
            Self::Home { tab } => format!("news:home:{tab}"),
            Self::Channel { channel_id } => format!("news:channel:{channel_id}"),
            Self::Company { code, kind } => {
                format!("news:code:{code}:{}", normalized_company_kind(kind))
            }
        }
    }

    pub fn method(&self) -> &'static str {
        match self {
            Self::Home { .. } => "f10.market_news",
            Self::Channel { .. } => "f10.channel_news",
            Self::Company { kind, .. } => match normalized_company_kind(kind) {
                "announcement" => "f10.announcements",
                "roadshow" => "f10.roadshows",
                "digest" => "f10.digest",
                _ => "f10.company_news",
            },
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Home { .. } | Self::Channel { .. } => "news",
            Self::Company { kind, .. } => normalized_company_kind(kind),
        }
    }

    pub fn code(&self) -> Option<&str> {
        match self {
            Self::Company { code, .. } => Some(code),
            _ => None,
        }
    }

    pub fn channel_id(&self) -> Option<i64> {
        match self {
            Self::Channel { channel_id } => Some(*channel_id),
            _ => None,
        }
    }

    /// Company feeds other than research reports are returned in one full page.
    pub fn paginated(&self) -> bool {
        match self {
            Self::Home { .. } | Self::Channel { .. } => true,
            Self::Company { kind, .. } => normalized_company_kind(kind) == "news",
        }
    }

    /// Digest rows only carry `MM-DD`, so they cannot drive a year-safe
    /// watermark; those feeds rely on all-seen pagination instead.
    pub fn watermarkable(&self) -> bool {
        !matches!(
            self,
            Self::Company { kind, .. } if normalized_company_kind(kind) == "digest"
        )
    }

    /// Market feeds put articles in the second table; cache/digest/research
    /// feeds put them in the first.
    pub fn result_set_index(&self) -> usize {
        match self {
            Self::Home { .. } | Self::Channel { .. } => 1,
            Self::Company { .. } => 0,
        }
    }

    pub fn params(&self, page: i64, page_size: i64) -> Value {
        match self {
            Self::Home { tab } => {
                json!({"tab": tab, "page": page, "page_size": page_size})
            }
            Self::Channel { channel_id } => json!({
                "channel": channel_id,
                "page": page,
                "page_size": page_size,
                "query1": "1",
                "keyword": "",
            }),
            Self::Company { code, kind } => match normalized_company_kind(kind) {
                "news" => json!({
                    "code": code,
                    "page": page,
                    "page_size": page_size,
                }),
                "digest" => json!({"code": code, "section": "news"}),
                _ => json!({"code": code}),
            },
        }
    }
}

fn normalized_company_kind(kind: &str) -> &'static str {
    match kind.trim().to_ascii_lowercase().as_str() {
        "gg" | "announcement" | "announcements" => "announcement",
        "ly" | "roadshow" | "roadshows" => "roadshow",
        "digest" => "digest",
        _ => "news",
    }
}

#[derive(Debug, Clone)]
pub struct NewsPage {
    pub items: Vec<NewsArticleInput>,
    pub skipped: usize,
}

impl NewsPage {
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

/// Parse the article table for a scope. Market feeds use the second result set;
/// cache/digest/research feeds use the first. Malformed rows are counted in
/// `skipped` instead of failing the whole page.
pub fn parse_news_page(scope: &NewsScope, response: &Value, run_id: Option<i64>) -> NewsPage {
    let rows = response
        .get("result_sets")
        .and_then(Value::as_array)
        .and_then(|sets| sets.get(scope.result_set_index()))
        .and_then(|set| set.get("rows"))
        .and_then(Value::as_array);
    let mut page = NewsPage {
        items: Vec::new(),
        skipped: 0,
    };
    if let Some(rows) = rows {
        for row in rows {
            match article_from_row(scope, row, run_id) {
                Some(item) => page.items.push(item),
                None => page.skipped += 1,
            }
        }
    }
    page
}

struct RowFields {
    title: String,
    published_at: Option<String>,
    summary: Option<String>,
    info_src: Option<String>,
    url: Option<String>,
    attach_file: Option<String>,
    rec_id: Option<String>,
    is_research: bool,
}

fn article_fields(scope: &NewsScope, row: &Map<String, Value>) -> Option<RowFields> {
    match scope {
        NewsScope::Home { .. } | NewsScope::Channel { .. } => Some(RowFields {
            title: text(field(row, &["Title", "title"]))?,
            published_at: text(field(row, &["Issue_date", "issue_date", "published_at"])),
            summary: summary_text(field(row, &["Summary", "summary"])),
            info_src: text(field(row, &["Info_Src", "info_src", "Source", "source"])),
            url: text(field(row, &["Url", "url"])),
            attach_file: text(field(row, &["AttachFile", "attach_file"])),
            rec_id: text(field(row, &["rec_id", "Rec_id", "record_id"])),
            is_research: truthy(field(row, &["bProc", "is_research"])),
        }),
        NewsScope::Company { kind, .. } => match normalized_company_kind(kind) {
            "announcement" => Some(RowFields {
                title: text(field(row, &["title", "Title"]))?,
                published_at: text(field(row, &["issue_date", "Issue_date"])),
                summary: summary_text(field(row, &["summary", "Summary"])),
                info_src: text(field(row, &["source", "Source"])),
                url: text(field(row, &["url", "Url"])),
                attach_file: text(field(row, &["attach_file", "AttachFile"])),
                rec_id: text(field(row, &["rec_id", "Rec_id"])),
                is_research: false,
            }),
            "roadshow" => Some(RowFields {
                title: text(field(row, &["title", "Title"]))?,
                published_at: combine_date_time(
                    text(field(row, &["start_date"])),
                    text(field(row, &["start_time"])),
                )
                .or_else(|| text(field(row, &["issue_date", "Issue_date"]))),
                summary: summary_text(field(row, &["summary", "Summary"])),
                info_src: text(field(row, &["source", "Source"])),
                url: text(field(row, &["url", "Url"])),
                attach_file: text(field(row, &["attach_file", "AttachFile"])),
                rec_id: text(field(row, &["rec_id", "Rec_id"])),
                is_research: false,
            }),
            "digest" => Some(RowFields {
                title: text(field(row, &["Title", "title"]))?,
                published_at: text(field(row, &["rq", "Issue_date", "issue_date"])),
                summary: summary_text(field(row, &["Summary", "summary"])),
                info_src: text(field(row, &["Info_Src", "info_src", "source"])),
                url: text(field(row, &["Url", "url"])),
                attach_file: None,
                rec_id: text(field(row, &["rec_id", "Rec_id"])),
                is_research: false,
            }),
            _ => Some(RowFields {
                title: text(field(row, &["T039", "Title", "title"]))?,
                published_at: normalize_date(text(field(row, &["T012", "issue_date"]))),
                summary: None,
                info_src: None,
                url: None,
                attach_file: text(field(row, &["ybdz"])),
                rec_id: text(field(row, &["T011", "rec_id"])),
                is_research: true,
            }),
        },
    }
}

fn article_from_row(
    scope: &NewsScope,
    row: &Value,
    run_id: Option<i64>,
) -> Option<NewsArticleInput> {
    let row = row.as_object()?;
    let fields = article_fields(scope, row)?;

    let article_key = match &fields.rec_id {
        Some(id) => id.clone(),
        None => stable_key(&[
            &scope.scope_key(),
            &fields.title,
            fields.published_at.as_deref().unwrap_or_default(),
            fields.url.as_deref().unwrap_or_default(),
        ]),
    };

    let mut article = NewsArticleInput::new(
        scope.dataset(),
        NEWS_SOURCE,
        scope.scope_type(),
        scope.scope_key(),
        scope.kind(),
        fields.rec_id.clone().unwrap_or_else(|| article_key.clone()),
        article_key,
        fields.title,
    );
    article.code = scope.code().map(str::to_owned);
    article.channel_id = scope.channel_id();
    article.published_at = fields.published_at;
    article.summary = fields.summary;
    article.info_src = fields.info_src;
    article.url = fields.url;
    article.attach_file = fields.attach_file;
    article.is_research = fields.is_research;
    article.run_id = run_id;
    Some(article)
}

fn field<'a>(row: &'a Map<String, Value>, names: &[&str]) -> Option<&'a Value> {
    names.iter().find_map(|name| row.get(*name))
}

fn text(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::String(value)) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_owned())
            }
        }
        Some(Value::Number(value)) => Some(value.to_string()),
        Some(Value::Bool(value)) => Some(value.to_string()),
        _ => None,
    }
}

fn summary_text(value: Option<&Value>) -> Option<String> {
    text(value).filter(|value| !value.eq_ignore_ascii_case("null"))
}

/// Normalize compact `YYYYMMDD` dates while preserving already formatted text.
fn normalize_date(value: Option<String>) -> Option<String> {
    let value = value?;
    if value.len() == 8 && value.bytes().all(|byte| byte.is_ascii_digit()) {
        Some(format!(
            "{}-{}-{}",
            &value[0..4],
            &value[4..6],
            &value[6..8]
        ))
    } else {
        Some(value)
    }
}

fn combine_date_time(date: Option<String>, time: Option<String>) -> Option<String> {
    let date = normalize_date(date)?;
    match time {
        Some(time) => {
            let time = if time.len() == 5 {
                format!("{time}:00")
            } else {
                time
            };
            Some(format!("{date} {time}"))
        }
        None => Some(date),
    }
}

fn truthy(value: Option<&Value>) -> bool {
    match value {
        Some(Value::Bool(value)) => *value,
        Some(Value::Number(value)) => value.as_i64().is_some_and(|value| value != 0),
        Some(Value::String(value)) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "y"
        ),
        _ => false,
    }
}

fn stable_key(parts: &[&str]) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for part in parts {
        for byte in part.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("fnv1a:{hash:016x}")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    Continue,
    Empty,
    AllSeen,
    MaxPages,
    OlderThanWatermark,
    SinglePage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageDecision {
    pub stop: bool,
    pub reason: StopReason,
    pub new_items: usize,
}

/// Decide whether the next page should be requested. The caller adds the page's
/// keys to `existing_keys` after a successful storage commit.
pub fn decide_page(
    page: i64,
    max_pages: i64,
    items: &[NewsArticleInput],
    existing_keys: &HashSet<String>,
    watermark: Option<&str>,
) -> PageDecision {
    let new_items = items
        .iter()
        .filter(|item| !existing_keys.contains(&item.article_key))
        .count();

    if page >= max_pages.max(1) {
        return PageDecision {
            stop: true,
            reason: StopReason::MaxPages,
            new_items,
        };
    }
    if items.is_empty() {
        return PageDecision {
            stop: true,
            reason: StopReason::Empty,
            new_items,
        };
    }
    if new_items == 0 {
        return PageDecision {
            stop: true,
            reason: StopReason::AllSeen,
            new_items,
        };
    }
    if let Some(watermark) = watermark {
        let has_newer = items.iter().any(|item| {
            item.published_at
                .as_deref()
                .is_none_or(|published| published > watermark)
        });
        if !has_newer {
            return PageDecision {
                stop: true,
                reason: StopReason::OlderThanWatermark,
                new_items,
            };
        }
    }
    PageDecision {
        stop: false,
        reason: StopReason::Continue,
        new_items,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(scope: &NewsScope, title: &str, published: &str) -> NewsArticleInput {
        let mut article = NewsArticleInput::new(
            scope.dataset(),
            NEWS_SOURCE,
            scope.scope_type(),
            scope.scope_key(),
            scope.kind(),
            "rec-1",
            "key-1",
            title,
        );
        article.published_at = Some(published.to_owned());
        article
    }

    #[test]
    fn scopes_map_to_datasets_and_methods() {
        let home = NewsScope::Home { tab: 3 };
        assert_eq!(home.dataset(), "news.market");
        assert_eq!(home.method(), "f10.market_news");
        assert_eq!(home.scope_key(), "news:home:3");

        let channel = NewsScope::Channel { channel_id: 201 };
        assert_eq!(channel.dataset(), "news.market");
        assert_eq!(channel.method(), "f10.channel_news");
        assert_eq!(channel.channel_id(), Some(201));

        let notice = NewsScope::Company {
            code: "000001".to_owned(),
            kind: "gg".to_owned(),
        };
        assert_eq!(notice.dataset(), "news.announcement");
        assert_eq!(notice.method(), "f10.announcements");
        assert_eq!(notice.scope_key(), "news:code:000001:announcement");
        assert_eq!(notice.code(), Some("000001"));
        assert_eq!(notice.result_set_index(), 0);
        assert!(!notice.paginated());
        assert_eq!(notice.params(1, 20), json!({"code": "000001"}));

        let digest = NewsScope::Company {
            code: "000001".to_owned(),
            kind: "digest".to_owned(),
        };
        assert_eq!(digest.dataset(), "news.company");
        assert_eq!(
            digest.params(1, 20),
            json!({"code": "000001", "section": "news"})
        );
        assert!(!digest.watermarkable());

        let research = NewsScope::Company {
            code: "000001".to_owned(),
            kind: "news".to_owned(),
        };
        assert!(research.paginated());
        assert_eq!(
            research.params(2, 20),
            json!({"code": "000001", "page": 2, "page_size": 20})
        );
    }

    #[test]
    fn default_scopes_cover_home_tabs_and_channels() {
        let homes = home_scopes();
        assert_eq!(homes.len(), 8);
        assert_eq!(
            homes.iter().map(NewsScope::scope_key).collect::<Vec<_>>(),
            vec![
                "news:home:1",
                "news:home:2",
                "news:home:3",
                "news:home:4",
                "news:home:5",
                "news:home:6",
                "news:home:7",
                "news:home:9",
            ]
        );

        let channels = channel_scopes([101, 201]);
        assert_eq!(channels.len(), 2);
        assert_eq!(channels[1].scope_key(), "news:channel:201");
        assert_eq!(channels[1].method(), "f10.channel_news");
    }

    #[test]
    fn parses_company_feeds_from_the_first_table() {
        let announcement = NewsScope::Company {
            code: "000034".to_owned(),
            kind: "gg".to_owned(),
        };
        let response = json!({
            "result_sets": [
                {"rows": [{
                    "issue_date": "2026-05-26 00:00:00",
                    "title": "公告标题",
                    "rec_id": "1225329064",
                    "url": "https://example.test/notice.pdf",
                    "source": "交易所"
                }]},
                {"rows": [{"Title": "不该读取的第二张表"}]}
            ]
        });
        let page = parse_news_page(&announcement, &response, None);
        assert_eq!(page.items.len(), 1);
        let article = &page.items[0];
        assert_eq!(article.title, "公告标题");
        assert_eq!(article.dataset, "news.announcement");
        assert_eq!(article.published_at.as_deref(), Some("2026-05-26 00:00:00"));
        assert_eq!(
            article.url.as_deref(),
            Some("https://example.test/notice.pdf")
        );
        assert_eq!(article.rec_id, "1225329064");

        let research = NewsScope::Company {
            code: "000034".to_owned(),
            kind: "news".to_owned(),
        };
        let response = json!({
            "result_sets": [{"rows": [{
                "T004": "买入",
                "T009": "研究员",
                "T012": "20261106",
                "T011": "1652570",
                "T039": "研报标题",
                "ybdz": "281D0F0A"
            }]}]
        });
        let page = parse_news_page(&research, &response, None);
        let article = &page.items[0];
        assert_eq!(article.title, "研报标题");
        assert_eq!(article.published_at.as_deref(), Some("2026-11-06"));
        assert_eq!(article.attach_file.as_deref(), Some("281D0F0A"));
        assert!(article.is_research);
        assert_eq!(article.rec_id, "1652570");

        let digest = NewsScope::Company {
            code: "000034".to_owned(),
            kind: "digest".to_owned(),
        };
        let response = json!({
            "result_sets": [{"rows": [{
                "rec_id": "22364122",
                "rq": "11-27",
                "Title": "公司新闻标题"
            }]}]
        });
        let page = parse_news_page(&digest, &response, None);
        assert_eq!(page.items[0].published_at.as_deref(), Some("11-27"));

        let roadshow = NewsScope::Company {
            code: "000034".to_owned(),
            kind: "ly".to_owned(),
        };
        let response = json!({
            "result_sets": [{"rows": [{
                "title": "路演标题",
                "start_date": "20130913",
                "start_time": "14:00",
                "summary": "null",
                "url": "https://example.test/roadshow"
            }]}]
        });
        let page = parse_news_page(&roadshow, &response, None);
        assert_eq!(
            page.items[0].published_at.as_deref(),
            Some("2013-09-13 14:00:00")
        );
        assert_eq!(page.items[0].summary, None);
    }

    #[test]
    fn parses_second_result_set_and_skips_malformed_rows() {
        let scope = NewsScope::Channel { channel_id: 201 };
        let response = json!({
            "result_sets": [
                {"rows": []},
                {"rows": [
                    {
                        "Title": " 市场要闻 ",
                        "Issue_date": "2026-11-28 09:30:00",
                        "Summary": "摘要",
                        "rec_id": "rec-1",
                        "Info_Src": "通达信",
                        "Url": "https://example.test/1",
                        "bProc": "1"
                    },
                    {"Issue_date": "2026-11-28 09:31:00"}
                ]}
            ]
        });

        let page = parse_news_page(&scope, &response, Some(7));
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.skipped, 1);
        let article = &page.items[0];
        assert_eq!(article.title, "市场要闻");
        assert_eq!(article.dataset, "news.market");
        assert_eq!(article.channel_id, Some(201));
        assert_eq!(article.article_key, "rec-1");
        assert!(article.is_research);
        assert_eq!(article.run_id, Some(7));
    }

    #[test]
    fn missing_record_id_uses_stable_fallback_key() {
        let scope = NewsScope::Home { tab: 1 };
        let response = json!({
            "result_sets": [
                {},
                {"rows": [{
                    "Title": "滚动资讯",
                    "Issue_date": "2026-11-28 10:00:00",
                    "Url": "https://example.test/2"
                }]}
            ]
        });

        let first = parse_news_page(&scope, &response, None);
        let second = parse_news_page(&scope, &response, None);
        assert_eq!(first.items.len(), 1);
        assert_eq!(first.items[0].article_key, second.items[0].article_key);
        assert!(first.items[0].article_key.starts_with("fnv1a:"));
    }

    #[test]
    fn page_decisions_cover_empty_seen_limit_and_watermark() {
        let scope = NewsScope::Channel { channel_id: 201 };
        let rows = vec![item(&scope, "标题", "2026-11-28 09:30:00")];

        let empty = decide_page(1, 20, &[], &HashSet::new(), None);
        assert_eq!(empty.reason, StopReason::Empty);
        assert!(empty.stop);

        let seen = HashSet::from(["key-1".to_owned()]);
        let all_seen = decide_page(1, 20, &rows, &seen, None);
        assert_eq!(all_seen.reason, StopReason::AllSeen);
        assert!(all_seen.stop);

        let next = decide_page(1, 20, &rows, &HashSet::new(), None);
        assert_eq!(next.reason, StopReason::Continue);
        assert!(!next.stop);
        assert_eq!(next.new_items, 1);

        let limit = decide_page(20, 20, &rows, &HashSet::new(), None);
        assert_eq!(limit.reason, StopReason::MaxPages);
        assert!(limit.stop);

        let old = decide_page(1, 20, &rows, &HashSet::new(), Some("2026-11-28 10:00:00"));
        assert_eq!(old.reason, StopReason::OlderThanWatermark);
        assert!(old.stop);
    }
}
