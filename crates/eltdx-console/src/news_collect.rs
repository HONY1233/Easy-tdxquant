//! Page loop that turns paginated 7615 news responses into stored articles.

use std::collections::HashSet;
use std::future::Future;

use eltdx_store::{NewsArticleInput, Store, StoreError};
use serde_json::Value;
use thiserror::Error;

use crate::news::{decide_page, parse_news_page, NewsScope, StopReason, NEWS_SOURCE};

/// Async RPC boundary so the page loop can be tested without a live gateway.
///
/// The console binary implements this for the gateway handle once it owns a
/// running data plane; tests use a deterministic mock.
pub trait NewsRpc {
    fn call(
        &self,
        method: &str,
        params: Value,
    ) -> impl Future<Output = Result<Value, String>> + Send;
}

#[derive(Debug, Clone)]
pub struct NewsRunOptions {
    pub page_size: i64,
    pub max_pages: i64,
    pub start_page: i64,
    /// Explicit watermark; when absent the stored dataset watermark is used.
    pub watermark: Option<String>,
    pub run_id: Option<i64>,
    /// Write the highest published timestamp back to `dataset_state`.
    pub advance_watermark: bool,
}

impl Default for NewsRunOptions {
    fn default() -> Self {
        Self {
            page_size: 50,
            max_pages: 20,
            start_page: 1,
            watermark: None,
            run_id: None,
            advance_watermark: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewsRunReport {
    pub scope_key: String,
    pub pages: i64,
    pub items: usize,
    pub skipped: usize,
    pub inserted: usize,
    pub refreshed: usize,
    pub stop_reason: StopReason,
    pub watermark_after: Option<String>,
    pub watermark_advanced: bool,
}

#[derive(Debug, Error)]
pub enum NewsCollectError {
    #[error("news storage error: {0}")]
    Store(#[from] StoreError),
    #[error("news rpc error: {0}")]
    Rpc(String),
}

/// Fetch pages for one news scope until it is fully seen, empty, or reaches the
/// page cap. The watermark only advances when pagination finished naturally, so
/// a capped run can resume without skipping the remaining pages.
pub async fn collect_news_scope<R: NewsRpc + ?Sized>(
    store: &Store,
    rpc: &R,
    scope: &NewsScope,
    options: &NewsRunOptions,
) -> Result<NewsRunReport, NewsCollectError> {
    let scope_key = scope.scope_key();
    let watermark = if scope.watermarkable() {
        match &options.watermark {
            Some(value) => Some(value.clone()),
            None => store
                .get_dataset_state(scope.dataset(), &scope_key)?
                .and_then(|state| state.watermark),
        }
    } else {
        None
    };
    let mut existing_keys: HashSet<String> = store
        .list_news_keys(NEWS_SOURCE, &scope_key)?
        .into_iter()
        .collect();
    let max_pages = options.max_pages.max(1);
    let start_page = options.start_page.max(1);
    let mut page = start_page;
    let mut pages = 0_i64;
    let mut items = 0_usize;
    let mut skipped = 0_usize;
    let mut inserted = 0_usize;
    let mut refreshed = 0_usize;
    let mut watermark_after = watermark.clone();
    let stop_reason;

    loop {
        let response = rpc
            .call(scope.method(), scope.params(page, options.page_size))
            .await
            .map_err(NewsCollectError::Rpc)?;
        let parsed = parse_news_page(scope, &response, options.run_id);
        let decision = decide_page(
            page,
            max_pages,
            &parsed.items,
            &existing_keys,
            watermark.as_deref(),
        );

        pages += 1;
        items += parsed.items.len();
        skipped += parsed.skipped;
        for article in &parsed.items {
            let is_new = !existing_keys.contains(&article.article_key);
            store.upsert_news_article(article)?;
            if is_new {
                inserted += 1;
                existing_keys.insert(article.article_key.clone());
            } else {
                refreshed += 1;
            }
            watermark_after = latest_published(watermark_after, article);
        }

        if decision.stop {
            stop_reason = if !scope.paginated() && decision.reason == StopReason::MaxPages {
                StopReason::SinglePage
            } else {
                decision.reason
            };
            break;
        }
        if !scope.paginated() {
            stop_reason = StopReason::SinglePage;
            break;
        }
        page += 1;
    }

    let mut watermark_advanced = false;
    if options.advance_watermark
        && scope.watermarkable()
        && stop_reason != StopReason::MaxPages
        && options.run_id.is_some()
        && watermark_after.as_deref() != watermark.as_deref()
    {
        if let (Some(run_id), Some(value)) = (options.run_id, watermark_after.as_deref()) {
            store.advance_watermark(scope.dataset(), &scope_key, value, run_id)?;
            watermark_advanced = true;
        }
    }

    Ok(NewsRunReport {
        scope_key,
        pages,
        items,
        skipped,
        inserted,
        refreshed,
        stop_reason,
        watermark_after,
        watermark_advanced,
    })
}

fn latest_published(current: Option<String>, article: &NewsArticleInput) -> Option<String> {
    let Some(published) = article.published_at.as_deref() else {
        return current;
    };
    match current {
        Some(value) if value.as_str() >= published => Some(value),
        _ => Some(published.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use eltdx_store::Store;
    use serde_json::{json, Value};

    use super::*;

    struct MockRpc {
        pages: Vec<Value>,
        calls: Mutex<usize>,
    }

    impl MockRpc {
        fn new(pages: Vec<Value>) -> Self {
            Self {
                pages,
                calls: Mutex::new(0),
            }
        }

        fn call_count(&self) -> Result<usize, String> {
            self.calls
                .lock()
                .map(|calls| *calls)
                .map_err(|_| "mock lock poisoned".to_owned())
        }
    }

    impl NewsRpc for MockRpc {
        async fn call(&self, _method: &str, _params: Value) -> Result<Value, String> {
            let index = {
                let mut calls = self
                    .calls
                    .lock()
                    .map_err(|_| "mock lock poisoned".to_owned())?;
                let index = *calls;
                *calls += 1;
                index
            };
            self.pages
                .get(index)
                .cloned()
                .ok_or_else(|| "no more mock pages".to_owned())
        }
    }

    fn page(rows: Value) -> Value {
        json!({"result_sets": [{}, {"rows": rows}]})
    }

    fn first_page(rows: Value) -> Value {
        json!({"result_sets": [{"rows": rows}]})
    }

    fn row(rec_id: &str, title: &str, published: &str) -> Value {
        json!({
            "rec_id": rec_id,
            "Title": title,
            "Issue_date": published,
            "Summary": "摘要",
            "Info_Src": "通达信",
        })
    }

    #[tokio::test]
    async fn collects_until_all_seen_and_advances_watermark(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let store = Store::open_in_memory()?;
        let scope = NewsScope::Channel { channel_id: 201 };
        let rpc = MockRpc::new(vec![
            page(json!([row("a", "标题A", "2026-11-28 10:00:00")])),
            page(json!([
                row("a", "标题A", "2026-11-28 10:00:00"),
                row("b", "标题B", "2026-11-28 09:00:00")
            ])),
            page(json!([
                row("a", "标题A", "2026-11-28 10:00:00"),
                row("b", "标题B", "2026-11-28 09:00:00")
            ])),
        ]);
        let options = NewsRunOptions {
            run_id: Some(1),
            ..NewsRunOptions::default()
        };

        let report = collect_news_scope(&store, &rpc, &scope, &options).await?;

        assert_eq!(report.pages, 3);
        assert_eq!(report.items, 5);
        assert_eq!(report.inserted, 2);
        assert_eq!(report.refreshed, 3);
        assert_eq!(report.stop_reason, StopReason::AllSeen);
        assert!(report.watermark_advanced);
        assert_eq!(
            report.watermark_after.as_deref(),
            Some("2026-11-28 10:00:00")
        );
        assert_eq!(rpc.call_count()?, 3);
        assert_eq!(store.count_news_articles()?, 2);
        assert_eq!(
            store
                .get_dataset_state("news.market", "news:channel:201")?
                .and_then(|state| state.watermark)
                .as_deref(),
            Some("2026-11-28 10:00:00")
        );
        Ok(())
    }

    #[tokio::test]
    async fn capped_run_stops_and_keeps_watermark_for_resume(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let store = Store::open_in_memory()?;
        let scope = NewsScope::Channel { channel_id: 201 };
        let rpc = MockRpc::new(vec![
            page(json!([row("a", "标题A", "2026-11-28 10:00:00")])),
            page(json!([row("b", "标题B", "2026-11-28 09:00:00")])),
        ]);
        let options = NewsRunOptions {
            max_pages: 2,
            run_id: Some(1),
            ..NewsRunOptions::default()
        };

        let report = collect_news_scope(&store, &rpc, &scope, &options).await?;

        assert_eq!(report.pages, 2);
        assert_eq!(report.inserted, 2);
        assert_eq!(report.stop_reason, StopReason::MaxPages);
        assert!(!report.watermark_advanced);
        assert_eq!(rpc.call_count()?, 2);
        assert!(store
            .get_dataset_state("news.market", "news:channel:201")?
            .is_none());
        Ok(())
    }

    #[tokio::test]
    async fn digest_feed_keeps_raw_date_without_watermark() -> Result<(), Box<dyn std::error::Error>>
    {
        let store = Store::open_in_memory()?;
        let scope = NewsScope::Company {
            code: "000034".to_owned(),
            kind: "digest".to_owned(),
        };
        let rpc = MockRpc::new(vec![first_page(json!([{
            "rec_id": "22364122",
            "rq": "11-27",
            "Title": "公司新闻标题"
        }]))]);

        let report = collect_news_scope(
            &store,
            &rpc,
            &scope,
            &NewsRunOptions {
                run_id: Some(1),
                ..NewsRunOptions::default()
            },
        )
        .await?;

        assert_eq!(report.stop_reason, StopReason::SinglePage);
        assert_eq!(report.watermark_after.as_deref(), Some("11-27"));
        assert!(!report.watermark_advanced);
        assert!(store
            .get_dataset_state("news.company", "news:code:000034:digest")?
            .is_none());
        Ok(())
    }

    #[tokio::test]
    async fn single_page_company_feed_stops_after_one_request(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let store = Store::open_in_memory()?;
        let scope = NewsScope::Company {
            code: "000034".to_owned(),
            kind: "gg".to_owned(),
        };
        let rpc = MockRpc::new(vec![first_page(json!([{
            "rec_id": "1225329064",
            "title": "公告标题",
            "issue_date": "2026-05-26 00:00:00"
        }]))]);
        let options = NewsRunOptions {
            run_id: Some(1),
            ..NewsRunOptions::default()
        };

        let report = collect_news_scope(&store, &rpc, &scope, &options).await?;

        assert_eq!(report.pages, 1);
        assert_eq!(report.stop_reason, StopReason::SinglePage);
        assert!(report.watermark_advanced);
        assert_eq!(rpc.call_count()?, 1);
        assert_eq!(store.count_news_articles()?, 1);
        Ok(())
    }
}
