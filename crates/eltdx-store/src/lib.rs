//! SQLite persistence for the Easy-tdxquant local console.
//!
//! The store owns one serialized writer connection. Callers should invoke it
//! from `spawn_blocking` in async code and keep transactions short.

pub mod error;
pub mod model;
pub mod schema;
mod store;

pub use error::StoreError;
pub use model::{
    AdminUser, AuditEntry, ChangeEntry, Checkpoint, CollectionMode, CollectionTask, DatasetState,
    NewTask, NewsArticle, NewsArticleInput, NewsBody, NewsChannel, RunStatus,
};
pub use store::{Result, Store};

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use serde_json::json;

    use super::*;

    fn store() -> Store {
        match Store::open_in_memory() {
            Ok(store) => store,
            Err(error) => panic!("open in-memory store: {error}"),
        }
    }

    fn temp_path(name: &str) -> PathBuf {
        let nanos = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_nanos(),
            Err(_) => 0,
        };
        std::env::temp_dir().join(format!("eltdx-store-{}-{nanos}-{name}", std::process::id()))
    }

    #[test]
    fn migration_is_idempotent_and_seeds_settings() -> Result<()> {
        let store = store();
        assert_eq!(store.schema_version()?, 2);
        assert_eq!(store.get_setting("console.port")?, Some("8787".to_owned()));
        assert_eq!(
            store.get_setting("data.retention.raw_days")?,
            Some("7".to_owned())
        );
        assert_eq!(
            store.get_setting("data.retention.news_days")?,
            Some("365".to_owned())
        );

        store.set_setting("console.port", "8790")?;
        assert_eq!(store.get_setting("console.port")?, Some("8790".to_owned()));
        store.migrate()?;
        assert_eq!(store.schema_version()?, 2);
        Ok(())
    }

    #[test]
    fn admin_passwords_use_argon2id_and_reject_wrong_input() -> Result<()> {
        let store = store();
        assert!(!store.has_admin()?);
        let id = store.create_admin("admin", "correct horse battery")?;
        assert!(id > 0);
        assert!(store.has_admin()?);

        let user = store.verify_admin("admin", "correct horse battery")?;
        assert_eq!(user.as_ref().map(|user| user.role.as_str()), Some("admin"));
        assert!(store.verify_admin("admin", "wrong password")?.is_none());
        assert!(store
            .verify_admin("nobody", "correct horse battery")?
            .is_none());
        Ok(())
    }

    #[test]
    fn tasks_round_trip_and_toggle() -> Result<()> {
        let store = store();
        let mut task = NewTask::new(
            "daily kline",
            7709,
            "kline",
            "bars.kline",
            json!({"code": "sz000001"}),
            CollectionMode::Incremental,
        );
        task.overlap_days = 3;
        task.watermark_type = Some("trade_date".to_owned());
        let id = store.create_task(&task)?;

        let loaded = store.get_task(id)?;
        let loaded = match loaded {
            Some(task) => task,
            None => panic!("task must exist"),
        };
        assert_eq!(loaded.name, "daily kline");
        assert_eq!(loaded.port, 7709);
        assert_eq!(loaded.mode, CollectionMode::Incremental);
        assert_eq!(loaded.params["code"], "sz000001");
        assert!(loaded.enabled);

        store.set_task_enabled(id, false)?;
        let loaded = store.get_task(id)?;
        assert_eq!(loaded.map(|task| task.enabled), Some(false));
        assert_eq!(store.list_tasks()?.len(), 1);
        Ok(())
    }

    #[test]
    fn runs_checkpoints_and_watermarks_are_persisted() -> Result<()> {
        let store = store();
        let task = NewTask::new(
            "backfill",
            7709,
            "kline",
            "bars.kline",
            json!({}),
            CollectionMode::Full,
        );
        let task_id = store.create_task(&task)?;
        let run_id = store.start_run(
            task_id,
            "manual",
            CollectionMode::Full,
            Some("2026-01-01"),
            Some("2026-11-28"),
            None,
            None,
        )?;
        store.save_checkpoint(run_id, "shard-1", &json!({"page": 3}), "running", 300)?;
        store.save_checkpoint(run_id, "shard-1", &json!({"page": 4}), "done", 400)?;

        let checkpoints = store.list_checkpoints(run_id)?;
        assert_eq!(checkpoints.len(), 1);
        assert_eq!(checkpoints[0].cursor["page"], 4);
        assert_eq!(checkpoints[0].status, "done");

        store.advance_watermark("kline", "sz000001:day:qfq", "2026-11-27", run_id)?;
        let state = store.get_dataset_state("kline", "sz000001:day:qfq")?;
        let state = match state {
            Some(state) => state,
            None => panic!("dataset state must exist"),
        };
        assert_eq!(state.watermark.as_deref(), Some("2026-11-27"));
        assert_eq!(state.status, "success");
        assert_eq!(state.last_run_id, Some(run_id));

        store.mark_full("kline", "sz000001:day:qfq", run_id, 25)?;
        let state = match store.get_dataset_state("kline", "sz000001:day:qfq")? {
            Some(state) => state,
            None => panic!("dataset state must exist"),
        };
        assert!(state.last_full_at.is_some());
        assert_eq!(state.rows, 25);

        store.finish_run(
            run_id,
            RunStatus::Success,
            400,
            8192,
            None,
            Some("2026-11-27"),
        )?;
        Ok(())
    }

    #[test]
    fn payload_and_kline_upserts_are_idempotent() -> Result<()> {
        let store = store();
        store.upsert_payload(
            None,
            7709,
            "quote",
            "sz000001",
            "2026-11-28",
            "hash",
            &json!({"price": 10.0}),
        )?;
        store.upsert_payload(
            None,
            7709,
            "quote",
            "sz000001",
            "2026-11-28",
            "hash",
            &json!({"price": 10.5}),
        )?;
        assert_eq!(store.count_payloads()?, 1);

        store.upsert_kline(
            "sz000001",
            "7709",
            "day",
            "qfq",
            "2026-11-28",
            10.0,
            11.0,
            9.5,
            10.5,
            100.0,
            1000.0,
            None,
        )?;
        store.upsert_kline(
            "sz000001",
            "7709",
            "day",
            "qfq",
            "2026-11-28",
            10.0,
            11.2,
            9.5,
            10.8,
            120.0,
            1200.0,
            None,
        )?;
        assert_eq!(store.count_klines()?, 1);

        store.upsert_quote_snapshot(
            "sz000001",
            "2026-11-28T09:30:00Z",
            10.5,
            10.0,
            10.1,
            10.8,
            10.0,
            1000.0,
            10500.0,
            Some(&json!({"bid": [[10.49, 100]]})),
            None,
        )?;
        store.upsert_quote_snapshot(
            "sz000001",
            "2026-11-28T09:30:00Z",
            10.6,
            10.0,
            10.1,
            10.8,
            10.0,
            1100.0,
            11660.0,
            None,
            None,
        )?;
        assert_eq!(store.count_quote_snapshots()?, 1);
        Ok(())
    }

    #[test]
    fn news_channels_articles_and_bodies_round_trip() -> Result<()> {
        let store = store();
        store.upsert_news_channel("tdx", 201, "财经要闻", Some("财经"), true)?;
        store.upsert_news_channel("tdx", 201, "财经要闻（更新）", Some("财经"), true)?;
        let channels = store.list_news_channels(true)?;
        assert_eq!(channels.len(), 1);
        assert_eq!(channels[0].name, "财经要闻（更新）");

        let mut article = NewsArticleInput::new(
            "news.market",
            "tdx",
            "channel",
            "news:channel:201",
            "news",
            "rec-1",
            "article-1",
            "市场要闻标题",
        );
        article.channel_id = Some(201);
        article.published_at = Some("2026-11-28 09:30:00".to_owned());
        article.summary = Some("摘要".to_owned());
        article.info_src = Some("通达信".to_owned());
        article.content_hash = Some("hash-1".to_owned());

        let id = store.upsert_news_article(&article)?;
        assert_eq!(store.upsert_news_article(&article)?, id);
        assert_eq!(store.count_news_articles()?, 1);
        assert_eq!(store.latest_change_seq()?, 1);

        let mut changed = article.clone();
        changed.title = "市场要闻标题（修订）".to_owned();
        changed.content_hash = Some("hash-2".to_owned());
        assert_eq!(store.upsert_news_article(&changed)?, id);
        assert_eq!(store.count_news_articles()?, 1);
        assert_eq!(store.latest_change_seq()?, 2);

        let loaded = match store.get_news_article_by_key("tdx", "news:channel:201", "article-1")? {
            Some(article) => article,
            None => panic!("news article must exist"),
        };
        assert_eq!(loaded.id, id);
        assert_eq!(loaded.title, "市场要闻标题（修订）");
        assert_eq!(loaded.channel_id, Some(201));
        assert_eq!(store.list_news(Some("news.market"), None, 10)?.len(), 1);
        assert!(store.list_news(Some("news.company"), None, 10)?.is_empty());

        store.upsert_news_body(id, "sjqd", "第一版正文", Some("body-1"), None)?;
        store.upsert_news_body(id, "sjqd", "第二版正文", Some("body-2"), None)?;
        assert_eq!(store.count_news_bodies()?, 1);
        let body = match store.get_news_body(id, "sjqd")? {
            Some(body) => body,
            None => panic!("news body must exist"),
        };
        assert_eq!(body.content, "第二版正文");
        assert_eq!(body.content_hash.as_deref(), Some("body-2"));

        assert_eq!(store.prune_news_before("9999-01-01")?, 1);
        assert_eq!(store.count_news_articles()?, 0);
        assert_eq!(store.count_news_bodies()?, 0);
        Ok(())
    }

    #[test]
    fn change_log_supports_cursor_replay_and_retention() -> Result<()> {
        let store = store();
        let first =
            store.append_change("quote", "sz000001", "upsert", "sz000001", &json!({"v": 1}))?;
        let second =
            store.append_change("quote", "sz000002", "upsert", "sz000002", &json!({"v": 2}))?;
        assert_eq!(first, 1);
        assert_eq!(second, 2);
        assert_eq!(store.latest_change_seq()?, 2);

        let all = store.recent_changes(0, 10)?;
        assert_eq!(all.len(), 2);
        let after = store.recent_changes(first, 10)?;
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].scope_key, "sz000002");

        assert_eq!(store.prune_change_log_keep(1)?, 1);
        let kept = store.recent_changes(0, 10)?;
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].seq, second);
        Ok(())
    }

    #[test]
    fn audit_log_round_trips() -> Result<()> {
        let store = store();
        store.append_audit(
            "admin",
            "task.create",
            "task:1",
            &json!({"name": "daily"}),
            Some("127.0.0.1"),
        )?;
        let entries = store.recent_audit(10)?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].actor, "admin");
        assert_eq!(entries[0].action, "task.create");
        assert_eq!(entries[0].detail["name"], "daily");
        Ok(())
    }

    #[test]
    fn backup_contains_committed_state() -> Result<()> {
        let source_path = temp_path("source.db");
        let backup_path = temp_path("backup.db");
        let source = Store::open(&source_path)?;
        source.set_setting("backup.marker", "present")?;
        source.backup_to(&backup_path)?;
        drop(source);

        let backup = Store::open(&backup_path)?;
        assert_eq!(
            backup.get_setting("backup.marker")?,
            Some("present".to_owned())
        );
        drop(backup);

        remove_database(&source_path);
        remove_database(&backup_path);
        Ok(())
    }

    #[test]
    fn restore_replaces_the_target_database() -> Result<()> {
        let backup_path = temp_path("restore-source.db");
        let target_path = temp_path("restore-target.db");
        let source = Store::open(&backup_path)?;
        source.set_setting("restore.marker", "from-backup")?;
        drop(source);

        let target = Store::open(&target_path)?;
        target.set_setting("restore.marker", "old")?;
        drop(target);

        Store::restore_database(&backup_path, &target_path)?;
        let restored = Store::open(&target_path)?;
        assert_eq!(
            restored.get_setting("restore.marker")?,
            Some("from-backup".to_owned())
        );
        drop(restored);

        remove_database(&backup_path);
        remove_database(&target_path);
        Ok(())
    }

    fn remove_database(path: &std::path::Path) {
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(path.with_extension("db-wal"));
        let _ = fs::remove_file(path.with_extension("db-shm"));
    }
}
