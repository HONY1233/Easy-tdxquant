use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::StoreError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CollectionMode {
    Full,
    Incremental,
    Backfill,
    Refresh,
}

impl CollectionMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Incremental => "incremental",
            Self::Backfill => "backfill",
            Self::Refresh => "refresh",
        }
    }
}

impl FromStr for CollectionMode {
    type Err = StoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "full" => Ok(Self::Full),
            "incremental" => Ok(Self::Incremental),
            "backfill" => Ok(Self::Backfill),
            "refresh" => Ok(Self::Refresh),
            other => Err(StoreError::Invalid(format!(
                "unknown collection mode: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Running,
    Success,
    Failed,
    Cancelled,
    Paused,
}

impl RunStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Success => "success",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Paused => "paused",
        }
    }
}

impl FromStr for RunStatus {
    type Err = StoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "running" => Ok(Self::Running),
            "success" => Ok(Self::Success),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            "paused" => Ok(Self::Paused),
            other => Err(StoreError::Invalid(format!("unknown run status: {other}"))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NewTask {
    pub name: String,
    pub port: u16,
    pub dataset: String,
    pub method: String,
    pub params: Value,
    pub mode: CollectionMode,
    pub watermark_type: Option<String>,
    pub overlap_days: i64,
    pub shard_size: i64,
    pub max_pages: Option<i64>,
    pub max_records: Option<i64>,
    pub schedule_kind: String,
    pub schedule_expr: Option<String>,
    pub timeout_ms: i64,
    pub retry: i64,
    pub concurrency: i64,
    pub retention_days: Option<i64>,
}

impl NewTask {
    pub fn new(
        name: impl Into<String>,
        port: u16,
        dataset: impl Into<String>,
        method: impl Into<String>,
        params: Value,
        mode: CollectionMode,
    ) -> Self {
        Self {
            name: name.into(),
            port,
            dataset: dataset.into(),
            method: method.into(),
            params,
            mode,
            watermark_type: None,
            overlap_days: 0,
            shard_size: 100,
            max_pages: None,
            max_records: None,
            schedule_kind: "manual".to_owned(),
            schedule_expr: None,
            timeout_ms: 8_000,
            retry: 2,
            concurrency: 1,
            retention_days: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CollectionTask {
    pub id: i64,
    pub name: String,
    pub port: u16,
    pub dataset: String,
    pub method: String,
    pub params: Value,
    pub mode: CollectionMode,
    pub watermark_type: Option<String>,
    pub overlap_days: i64,
    pub shard_size: i64,
    pub max_pages: Option<i64>,
    pub max_records: Option<i64>,
    pub schedule_kind: String,
    pub schedule_expr: Option<String>,
    pub enabled: bool,
    pub timeout_ms: i64,
    pub retry: i64,
    pub concurrency: i64,
    pub retention_days: Option<i64>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct DatasetState {
    pub dataset: String,
    pub scope_key: String,
    pub watermark: Option<String>,
    pub last_full_at: Option<String>,
    pub last_incremental_at: Option<String>,
    pub last_run_id: Option<i64>,
    pub status: String,
    pub rows: i64,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct Checkpoint {
    pub run_id: i64,
    pub shard_key: String,
    pub cursor: Value,
    pub status: String,
    pub rows: i64,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct AdminUser {
    pub id: i64,
    pub username: String,
    pub role: String,
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub struct AuditEntry {
    pub id: i64,
    pub ts: String,
    pub actor: String,
    pub action: String,
    pub target: String,
    pub detail: Value,
    pub ip: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ChangeEntry {
    pub seq: i64,
    pub dataset: String,
    pub scope_key: String,
    pub op: String,
    pub row_key: String,
    pub payload: Value,
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub struct NewsChannel {
    pub source: String,
    pub channel_id: i64,
    pub name: String,
    pub group_name: Option<String>,
    pub enabled: bool,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct NewsArticleInput {
    pub dataset: String,
    pub source: String,
    pub scope_type: String,
    pub scope_key: String,
    pub code: Option<String>,
    pub channel_id: Option<i64>,
    pub kind: String,
    pub rec_id: String,
    pub article_key: String,
    pub title: String,
    pub published_at: Option<String>,
    pub summary: Option<String>,
    pub info_src: Option<String>,
    pub url: Option<String>,
    pub attach_file: Option<String>,
    pub is_research: bool,
    pub content_hash: Option<String>,
    pub run_id: Option<i64>,
}

impl NewsArticleInput {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        dataset: impl Into<String>,
        source: impl Into<String>,
        scope_type: impl Into<String>,
        scope_key: impl Into<String>,
        kind: impl Into<String>,
        rec_id: impl Into<String>,
        article_key: impl Into<String>,
        title: impl Into<String>,
    ) -> Self {
        Self {
            dataset: dataset.into(),
            source: source.into(),
            scope_type: scope_type.into(),
            scope_key: scope_key.into(),
            code: None,
            channel_id: None,
            kind: kind.into(),
            rec_id: rec_id.into(),
            article_key: article_key.into(),
            title: title.into(),
            published_at: None,
            summary: None,
            info_src: None,
            url: None,
            attach_file: None,
            is_research: false,
            content_hash: None,
            run_id: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct NewsArticle {
    pub id: i64,
    pub dataset: String,
    pub source: String,
    pub scope_type: String,
    pub scope_key: String,
    pub code: Option<String>,
    pub channel_id: Option<i64>,
    pub kind: String,
    pub rec_id: String,
    pub article_key: String,
    pub title: String,
    pub published_at: Option<String>,
    pub summary: Option<String>,
    pub info_src: Option<String>,
    pub url: Option<String>,
    pub attach_file: Option<String>,
    pub is_research: bool,
    pub content_hash: Option<String>,
    pub first_seen_at: String,
    pub last_seen_at: String,
    pub run_id: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct NewsBody {
    pub article_id: i64,
    pub detail_type: String,
    pub content: String,
    pub content_hash: Option<String>,
    pub fetched_at: String,
    pub run_id: Option<i64>,
}
