//! Retention policy: how long raw payloads, snapshots, news, audit entries and
//! change-log rows survive before they are pruned.
//!
//! `days < 0` disables a category (`-1` means "keep forever"); `days == 0`
//! prunes everything older than now, which is only useful in tests and for a
//! deliberate reset.

use std::sync::Arc;

use eltdx_store::{Store, StoreError};
use serde::Serialize;

use crate::clock::{format_iso_utc, now_epoch};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RetentionPolicy {
    pub raw_days: i64,
    pub snapshot_days: i64,
    pub news_days: i64,
    pub audit_days: i64,
    pub change_keep: i64,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            raw_days: 7,
            snapshot_days: 30,
            news_days: 365,
            audit_days: 90,
            change_keep: 10_000,
        }
    }
}

impl RetentionPolicy {
    /// Reads `<key>` settings, falling back to the defaults on absence or junk.
    pub fn from_settings(store: &Store) -> Result<Self, StoreError> {
        let mut policy = Self::default();
        if let Some(value) = read_i64(store, "data.retention.raw_days")? {
            policy.raw_days = value;
        }
        if let Some(value) = read_i64(store, "data.retention.snapshot_days")? {
            policy.snapshot_days = value;
        }
        if let Some(value) = read_i64(store, "data.retention.news_days")? {
            policy.news_days = value;
        }
        if let Some(value) = read_i64(store, "data.retention.audit_days")? {
            policy.audit_days = value;
        }
        if let Some(value) = read_i64(store, "data.retention.change_keep")? {
            policy.change_keep = value;
        }
        Ok(policy)
    }
}

fn read_i64(store: &Store, key: &str) -> Result<Option<i64>, StoreError> {
    match store.get_setting(key)? {
        Some(value) => Ok(value.trim().parse().ok()),
        None => Ok(None),
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct RetentionReport {
    pub payloads: usize,
    pub snapshots: usize,
    pub news_articles: usize,
    pub news_bodies: usize,
    pub audit: usize,
    pub changes: usize,
    pub vacuumed: bool,
}

/// Async wrapper: the store is synchronous, so pruning runs off the runtime.
pub async fn prune(
    store: &Arc<Store>,
    policy: RetentionPolicy,
) -> Result<RetentionReport, StoreError> {
    let store = Arc::clone(store);
    let now = now_epoch();
    tokio::task::spawn_blocking(move || prune_blocking(&store, policy, now))
        .await
        .map_err(|error| StoreError::Invalid(error.to_string()))?
}

pub fn prune_blocking(
    store: &Store,
    policy: RetentionPolicy,
    now: i64,
) -> Result<RetentionReport, StoreError> {
    let mut report = RetentionReport::default();
    if policy.raw_days >= 0 {
        report.payloads = store.prune_payloads_before(&cutoff(now, policy.raw_days))?;
    }
    if policy.snapshot_days >= 0 {
        report.snapshots =
            store.prune_quote_snapshots_before(&cutoff(now, policy.snapshot_days))?;
    }
    if policy.news_days >= 0 {
        let before = cutoff(now, policy.news_days);
        report.news_articles = store.prune_news_before(&before)?;
        report.news_bodies = store.prune_news_bodies_before(&before)?;
    }
    if policy.audit_days >= 0 {
        report.audit = store.prune_audit_before(&cutoff(now, policy.audit_days))?;
    }
    if policy.change_keep > 0 {
        report.changes = store.prune_change_log_keep(policy.change_keep)?;
    }
    store.incremental_vacuum()?;
    report.vacuumed = true;
    Ok(report)
}

fn cutoff(now: i64, days: i64) -> String {
    format_iso_utc(now.saturating_sub(days.saturating_mul(86_400)))
}

#[cfg(test)]
mod tests {
    use eltdx_store::Store;
    use serde_json::json;

    use super::*;

    fn store() -> Arc<Store> {
        match Store::open_in_memory() {
            Ok(store) => Arc::new(store),
            Err(error) => panic!("open store: {error}"),
        }
    }

    #[test]
    fn zero_days_prunes_everything_older_than_now() {
        let store = store();
        if let Err(error) = store.upsert_payload(
            None,
            7709,
            "quote.snapshot",
            "sz000001",
            "2026-11-28",
            "hash",
            &json!({"last": 12.3}),
        ) {
            panic!("seed payload: {error}");
        }
        if let Err(error) = store.append_audit("tester", "test", "target", &json!({}), None) {
            panic!("seed audit: {error}");
        }
        let policy = RetentionPolicy {
            raw_days: 0,
            audit_days: 0,
            ..RetentionPolicy::default()
        };
        // One second past "now" so rows written this second fall inside the
        // cutoff; the store compares timestamps with a strict `<`.
        let report = match prune_blocking(&store, policy, now_epoch() + 1) {
            Ok(report) => report,
            Err(error) => panic!("prune: {error}"),
        };
        assert_eq!(report.payloads, 1);
        assert_eq!(report.audit, 1);
        assert!(report.vacuumed);
        assert_eq!(store.count_payloads().unwrap_or(-1), 0);
    }

    #[test]
    fn generous_windows_keep_recent_rows() {
        let store = store();
        if let Err(error) = store.upsert_payload(
            None,
            7709,
            "quote.snapshot",
            "sz000001",
            "2026-11-28",
            "hash",
            &json!({"last": 12.3}),
        ) {
            panic!("seed payload: {error}");
        }
        let report = match prune_blocking(&store, RetentionPolicy::default(), now_epoch()) {
            Ok(report) => report,
            Err(error) => panic!("prune: {error}"),
        };
        assert_eq!(report.payloads, 0);
        assert_eq!(store.count_payloads().unwrap_or(-1), 1);
    }

    #[test]
    fn change_log_keeps_the_newest_entries() {
        let store = store();
        for index in 0..3 {
            if let Err(error) = store.append_change(
                "quote.snapshot",
                "sz000001",
                "upsert",
                &format!("row-{index}"),
                &json!({ "index": index }),
            ) {
                panic!("seed change: {error}");
            }
        }
        let policy = RetentionPolicy {
            change_keep: 1,
            raw_days: -1,
            snapshot_days: -1,
            news_days: -1,
            audit_days: -1,
        };
        let report = match prune_blocking(&store, policy, now_epoch()) {
            Ok(report) => report,
            Err(error) => panic!("prune: {error}"),
        };
        assert_eq!(report.changes, 2);
        assert_eq!(store.latest_change_seq().unwrap_or(-1), 3);
    }

    #[test]
    fn settings_override_defaults() {
        let store = store();
        if let Err(error) = store.set_setting("data.retention.audit_days", "5") {
            panic!("set setting: {error}");
        }
        if let Err(error) = store.set_setting("data.retention.change_keep", "junk") {
            panic!("set setting: {error}");
        }
        let policy = match RetentionPolicy::from_settings(&store) {
            Ok(policy) => policy,
            Err(error) => panic!("policy: {error}"),
        };
        assert_eq!(policy.audit_days, 5);
        assert_eq!(policy.change_keep, RetentionPolicy::default().change_keep);
        assert_eq!(policy.raw_days, 7);
    }
}
