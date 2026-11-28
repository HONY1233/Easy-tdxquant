use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use argon2::password_hash::phc::PasswordHash;
use argon2::password_hash::{PasswordHasher, PasswordVerifier};
use argon2::Argon2;
use rusqlite::{params, Connection, OptionalExtension, Row};
use serde_json::Value;

use crate::error::StoreError;
use crate::model::{
    AdminUser, AuditEntry, ChangeEntry, Checkpoint, CollectionMode, CollectionTask, DatasetState,
    NewTask, NewsArticle, NewsArticleInput, NewsBody, NewsChannel, RunStatus,
};
use crate::schema::{MIGRATION_1, MIGRATION_2, SCHEMA_VERSION};

pub type Result<T> = std::result::Result<T, StoreError>;

const NOW: &str = "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')";
const TASK_COLUMNS: &str = "id, name, port, dataset, method, params_json, mode, watermark_type, \
    overlap_days, shard_size, max_pages, max_records, schedule_kind, schedule_expr, enabled, \
    timeout_ms, retry, concurrency, retention_days, created_at, updated_at";
const NEWS_COLUMNS: &str = "id, dataset, source, scope_type, scope_key, code, channel_id, kind, \
    rec_id, article_key, title, published_at, summary, info_src, url, attach_file, is_research, \
    content_hash, first_seen_at, last_seen_at, run_id";

pub struct Store {
    path: PathBuf,
    conn: Mutex<Connection>,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        let conn = Connection::open(&path)?;
        Self::from_connection(conn, path)
    }

    pub fn open_in_memory() -> Result<Self> {
        Self::from_connection(Connection::open_in_memory()?, PathBuf::from(":memory:"))
    }

    fn from_connection(conn: Connection, path: PathBuf) -> Result<Self> {
        configure(&conn)?;
        let store = Self {
            path,
            conn: Mutex::new(conn),
        };
        store.migrate()?;
        Ok(store)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn conn(&self) -> Result<MutexGuard<'_, Connection>> {
        self.conn
            .lock()
            .map_err(|_| StoreError::Invalid("store lock poisoned".to_owned()))
    }

    pub fn schema_version(&self) -> Result<i64> {
        let conn = self.conn()?;
        Ok(conn.query_row("PRAGMA user_version", [], |row| row.get(0))?)
    }

    pub fn migrate(&self) -> Result<()> {
        let mut conn = self.conn()?;
        let version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if version > SCHEMA_VERSION {
            return Err(StoreError::Invalid(format!(
                "database schema {version} is newer than supported {SCHEMA_VERSION}"
            )));
        }
        if version == SCHEMA_VERSION {
            return Ok(());
        }

        let tx = conn.transaction()?;
        if version < 1 {
            tx.execute_batch(MIGRATION_1)?;
            tx.execute(
                "INSERT OR REPLACE INTO schema_migrations(version, applied_at) \
                 VALUES (?1, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
                params![1],
            )?;
        }
        if version < 2 {
            tx.execute_batch(MIGRATION_2)?;
            tx.execute(
                "INSERT OR REPLACE INTO schema_migrations(version, applied_at) \
                 VALUES (?1, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
                params![2],
            )?;
        }
        tx.execute_batch(&format!("PRAGMA user_version = {SCHEMA_VERSION};"))?;
        tx.commit()?;
        Ok(())
    }

    pub fn backup_to(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        let conn = self.conn()?;
        let mut target = Connection::open(path)?;
        let backup = rusqlite::backup::Backup::new(&conn, &mut target)
            .map_err(|error| StoreError::Backup(error.to_string()))?;
        backup
            .run_to_completion(100, Duration::from_millis(10), None)
            .map_err(|error| StoreError::Backup(error.to_string()))?;
        Ok(())
    }

    pub fn restore_database(backup: impl AsRef<Path>, target: impl AsRef<Path>) -> Result<()> {
        let source = Connection::open(backup)?;
        let mut target = Connection::open(target)?;
        let backup = rusqlite::backup::Backup::new(&source, &mut target)
            .map_err(|error| StoreError::Backup(error.to_string()))?;
        backup
            .run_to_completion(100, Duration::from_millis(10), None)
            .map_err(|error| StoreError::Backup(error.to_string()))?;
        Ok(())
    }

    pub fn set_setting(&self, key: &str, value: &str) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            &format!(
                "INSERT INTO settings(key, value, updated_at) VALUES (?1, ?2, {NOW}) \
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at"
            ),
            params![key, value],
        )?;
        Ok(())
    }

    pub fn get_setting(&self, key: &str) -> Result<Option<String>> {
        let conn = self.conn()?;
        Ok(conn
            .query_row(
                "SELECT value FROM settings WHERE key = ?1",
                params![key],
                |row| row.get(0),
            )
            .optional()?)
    }

    pub fn has_admin(&self) -> Result<bool> {
        let conn = self.conn()?;
        let count: i64 =
            conn.query_row("SELECT COUNT(*) FROM admin_users", [], |row| row.get(0))?;
        Ok(count > 0)
    }

    pub fn create_admin(&self, username: &str, password: &str) -> Result<i64> {
        if username.trim().is_empty() {
            return Err(StoreError::Invalid("username must not be empty".to_owned()));
        }
        if password.len() < 8 {
            return Err(StoreError::Invalid(
                "password must contain at least 8 characters".to_owned(),
            ));
        }
        let hash = hash_password(password)?;
        let conn = self.conn()?;
        conn.execute(
            &format!(
                "INSERT INTO admin_users(username, password_hash, role, created_at, updated_at) \
                 VALUES (?1, ?2, 'admin', {NOW}, {NOW})"
            ),
            params![username.trim(), hash],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn verify_admin(&self, username: &str, password: &str) -> Result<Option<AdminUser>> {
        let conn = self.conn()?;
        let row = conn
            .query_row(
                "SELECT id, username, password_hash, role, created_at \
                 FROM admin_users WHERE username = ?1",
                params![username.trim()],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                },
            )
            .optional()?;
        let Some((id, username, hash, role, created_at)) = row else {
            return Ok(None);
        };
        if verify_password(password, &hash)? {
            Ok(Some(AdminUser {
                id,
                username,
                role,
                created_at,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn create_task(&self, task: &NewTask) -> Result<i64> {
        validate_new_task(task)?;
        let params_json = serde_json::to_string(&task.params)?;
        let conn = self.conn()?;
        conn.execute(
            &format!(
                "INSERT INTO collection_tasks(name, port, dataset, method, params_json, mode, \
                 watermark_type, overlap_days, shard_size, max_pages, max_records, schedule_kind, \
                 schedule_expr, enabled, timeout_ms, retry, concurrency, retention_days, created_at, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, 1, ?14, ?15, ?16, ?17, {NOW}, {NOW})"
            ),
            params![
                task.name.trim(),
                i64::from(task.port),
                task.dataset.trim(),
                task.method.trim(),
                params_json,
                task.mode.as_str(),
                task.watermark_type,
                task.overlap_days,
                task.shard_size,
                task.max_pages,
                task.max_records,
                task.schedule_kind.trim(),
                task.schedule_expr,
                task.timeout_ms,
                task.retry,
                task.concurrency,
                task.retention_days,
            ],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn get_task(&self, id: i64) -> Result<Option<CollectionTask>> {
        let conn = self.conn()?;
        let mut statement = conn.prepare(&format!(
            "SELECT {TASK_COLUMNS} FROM collection_tasks WHERE id = ?1"
        ))?;
        let mut rows = statement.query(params![id])?;
        match rows.next()? {
            Some(row) => Ok(Some(task_from_row(row)?)),
            None => Ok(None),
        }
    }

    pub fn list_tasks(&self) -> Result<Vec<CollectionTask>> {
        let conn = self.conn()?;
        let mut statement = conn.prepare(&format!(
            "SELECT {TASK_COLUMNS} FROM collection_tasks ORDER BY id"
        ))?;
        let mut rows = statement.query([])?;
        let mut tasks = Vec::new();
        while let Some(row) = rows.next()? {
            tasks.push(task_from_row(row)?);
        }
        Ok(tasks)
    }

    pub fn set_task_enabled(&self, id: i64, enabled: bool) -> Result<()> {
        let conn = self.conn()?;
        let changed = conn.execute(
            &format!("UPDATE collection_tasks SET enabled = ?2, updated_at = {NOW} WHERE id = ?1"),
            params![id, i64::from(enabled)],
        )?;
        if changed == 0 {
            return Err(StoreError::Invalid(format!("task {id} not found")));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn start_run(
        &self,
        task_id: i64,
        trigger: &str,
        mode: CollectionMode,
        range_start: Option<&str>,
        range_end: Option<&str>,
        watermark_before: Option<&str>,
        resumed_from: Option<i64>,
    ) -> Result<i64> {
        let conn = self.conn()?;
        conn.execute(
            &format!(
                "INSERT INTO collection_runs(task_id, \"trigger\", mode, range_start, range_end, \
                 watermark_before, resumed_from, started_at, status) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, {NOW}, 'running')"
            ),
            params![
                task_id,
                trigger,
                mode.as_str(),
                range_start,
                range_end,
                watermark_before,
                resumed_from,
            ],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn finish_run(
        &self,
        run_id: i64,
        status: RunStatus,
        rows: i64,
        bytes: i64,
        error: Option<&str>,
        watermark_after: Option<&str>,
    ) -> Result<()> {
        let conn = self.conn()?;
        let changed = conn.execute(
            &format!(
                "UPDATE collection_runs SET finished_at = {NOW}, status = ?2, rows = ?3, bytes = ?4, \
                 error = ?5, watermark_after = ?6, \
                 duration_ms = CAST((julianday('now') - julianday(started_at)) * 86400000 AS INTEGER) \
                 WHERE id = ?1"
            ),
            params![run_id, status.as_str(), rows, bytes, error, watermark_after],
        )?;
        if changed == 0 {
            return Err(StoreError::Invalid(format!("run {run_id} not found")));
        }
        Ok(())
    }

    pub fn get_dataset_state(
        &self,
        dataset: &str,
        scope_key: &str,
    ) -> Result<Option<DatasetState>> {
        let conn = self.conn()?;
        let row = conn
            .query_row(
                "SELECT dataset, scope_key, watermark, last_full_at, last_incremental_at, \
                 last_run_id, status, rows, updated_at \
                 FROM dataset_state WHERE dataset = ?1 AND scope_key = ?2",
                params![dataset, scope_key],
                |row| {
                    Ok(DatasetState {
                        dataset: row.get(0)?,
                        scope_key: row.get(1)?,
                        watermark: row.get(2)?,
                        last_full_at: row.get(3)?,
                        last_incremental_at: row.get(4)?,
                        last_run_id: row.get(5)?,
                        status: row.get(6)?,
                        rows: row.get(7)?,
                        updated_at: row.get(8)?,
                    })
                },
            )
            .optional()?;
        Ok(row)
    }

    pub fn upsert_dataset_state(&self, state: &DatasetState) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            &format!(
                "INSERT INTO dataset_state(dataset, scope_key, watermark, last_full_at, \
                 last_incremental_at, last_run_id, status, rows, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, {NOW}) \
                 ON CONFLICT(dataset, scope_key) DO UPDATE SET \
                 watermark = excluded.watermark, last_full_at = excluded.last_full_at, \
                 last_incremental_at = excluded.last_incremental_at, \
                 last_run_id = excluded.last_run_id, status = excluded.status, \
                 rows = excluded.rows, updated_at = excluded.updated_at"
            ),
            params![
                state.dataset,
                state.scope_key,
                state.watermark,
                state.last_full_at,
                state.last_incremental_at,
                state.last_run_id,
                state.status,
                state.rows,
            ],
        )?;
        Ok(())
    }

    /// Advance an incremental watermark. Call only after the batch transaction commits.
    pub fn advance_watermark(
        &self,
        dataset: &str,
        scope_key: &str,
        watermark: &str,
        run_id: i64,
    ) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            &format!(
                "INSERT INTO dataset_state(dataset, scope_key, watermark, last_incremental_at, \
                 last_run_id, status, rows, updated_at) \
                 VALUES (?1, ?2, ?3, {NOW}, ?4, 'success', 0, {NOW}) \
                 ON CONFLICT(dataset, scope_key) DO UPDATE SET \
                 watermark = excluded.watermark, last_incremental_at = excluded.last_incremental_at, \
                 last_run_id = excluded.last_run_id, status = 'success', updated_at = excluded.updated_at"
            ),
            params![dataset, scope_key, watermark, run_id],
        )?;
        Ok(())
    }

    pub fn mark_full(&self, dataset: &str, scope_key: &str, run_id: i64, rows: i64) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            &format!(
                "INSERT INTO dataset_state(dataset, scope_key, last_full_at, last_run_id, status, \
                 rows, updated_at) VALUES (?1, ?2, {NOW}, ?3, 'success', ?4, {NOW}) \
                 ON CONFLICT(dataset, scope_key) DO UPDATE SET \
                 last_full_at = excluded.last_full_at, last_run_id = excluded.last_run_id, \
                 status = 'success', rows = excluded.rows, updated_at = excluded.updated_at"
            ),
            params![dataset, scope_key, run_id, rows],
        )?;
        Ok(())
    }

    pub fn save_checkpoint(
        &self,
        run_id: i64,
        shard_key: &str,
        cursor: &Value,
        status: &str,
        rows: i64,
    ) -> Result<()> {
        let cursor_json = serde_json::to_string(cursor)?;
        let conn = self.conn()?;
        conn.execute(
            &format!(
                "INSERT INTO collection_checkpoints(run_id, shard_key, cursor_json, status, rows, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, {NOW}) \
                 ON CONFLICT(run_id, shard_key) DO UPDATE SET \
                 cursor_json = excluded.cursor_json, status = excluded.status, \
                 rows = excluded.rows, updated_at = excluded.updated_at"
            ),
            params![run_id, shard_key, cursor_json, status, rows],
        )?;
        Ok(())
    }

    pub fn list_checkpoints(&self, run_id: i64) -> Result<Vec<Checkpoint>> {
        let conn = self.conn()?;
        let mut statement = conn.prepare(
            "SELECT run_id, shard_key, cursor_json, status, rows, updated_at \
             FROM collection_checkpoints WHERE run_id = ?1 ORDER BY shard_key",
        )?;
        let mut rows = statement.query(params![run_id])?;
        let mut checkpoints = Vec::new();
        while let Some(row) = rows.next()? {
            let cursor_json: String = row.get(2)?;
            checkpoints.push(Checkpoint {
                run_id: row.get(0)?,
                shard_key: row.get(1)?,
                cursor: serde_json::from_str(&cursor_json)?,
                status: row.get(3)?,
                rows: row.get(4)?,
                updated_at: row.get(5)?,
            });
        }
        Ok(checkpoints)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn upsert_payload(
        &self,
        run_id: Option<i64>,
        port: u16,
        dataset: &str,
        code: &str,
        trade_date: &str,
        params_hash: &str,
        payload: &Value,
    ) -> Result<()> {
        let payload_json = serde_json::to_string(payload)?;
        let conn = self.conn()?;
        conn.execute(
            &format!(
                "INSERT INTO collected_payloads(run_id, port, dataset, code, trade_date, params_hash, \
                 payload_json, fetched_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, {NOW}) \
                 ON CONFLICT(dataset, code, trade_date, params_hash) DO UPDATE SET \
                 run_id = excluded.run_id, port = excluded.port, payload_json = excluded.payload_json, \
                 fetched_at = excluded.fetched_at"
            ),
            params![
                run_id,
                i64::from(port),
                dataset,
                code,
                trade_date,
                params_hash,
                payload_json,
            ],
        )?;
        Ok(())
    }

    pub fn count_payloads(&self) -> Result<i64> {
        let conn = self.conn()?;
        Ok(
            conn.query_row("SELECT COUNT(*) FROM collected_payloads", [], |row| {
                row.get(0)
            })?,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn upsert_kline(
        &self,
        code: &str,
        source: &str,
        period: &str,
        adjust: &str,
        trade_date: &str,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        volume: f64,
        amount: f64,
        run_id: Option<i64>,
    ) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "INSERT INTO kline_bars(code, source, period, adjust, trade_date, open, high, low, close, \
             volume, amount, run_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12) \
             ON CONFLICT(code, source, period, adjust, trade_date) DO UPDATE SET \
             open = excluded.open, high = excluded.high, low = excluded.low, close = excluded.close, \
             volume = excluded.volume, amount = excluded.amount, run_id = excluded.run_id",
            params![
                code, source, period, adjust, trade_date, open, high, low, close, volume, amount,
                run_id
            ],
        )?;
        Ok(())
    }

    pub fn count_klines(&self) -> Result<i64> {
        let conn = self.conn()?;
        Ok(conn.query_row("SELECT COUNT(*) FROM kline_bars", [], |row| row.get(0))?)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn upsert_quote_snapshot(
        &self,
        code: &str,
        ts: &str,
        price: f64,
        pre_close: f64,
        open: f64,
        high: f64,
        low: f64,
        volume: f64,
        amount: f64,
        depth: Option<&Value>,
        run_id: Option<i64>,
    ) -> Result<()> {
        let depth_json = depth.map(serde_json::to_string).transpose()?;
        let conn = self.conn()?;
        conn.execute(
            "INSERT INTO quote_snapshots(code, ts, price, pre_close, open, high, low, volume, amount, \
             depth_json, run_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11) \
             ON CONFLICT(code, ts) DO UPDATE SET price = excluded.price, pre_close = excluded.pre_close, \
             open = excluded.open, high = excluded.high, low = excluded.low, volume = excluded.volume, \
             amount = excluded.amount, depth_json = excluded.depth_json, run_id = excluded.run_id",
            params![
                code, ts, price, pre_close, open, high, low, volume, amount, depth_json, run_id
            ],
        )?;
        Ok(())
    }

    pub fn count_quote_snapshots(&self) -> Result<i64> {
        let conn = self.conn()?;
        Ok(conn.query_row("SELECT COUNT(*) FROM quote_snapshots", [], |row| row.get(0))?)
    }

    pub fn append_change(
        &self,
        dataset: &str,
        scope_key: &str,
        op: &str,
        row_key: &str,
        payload: &Value,
    ) -> Result<i64> {
        let payload_json = serde_json::to_string(payload)?;
        let conn = self.conn()?;
        conn.execute(
            &format!(
                "INSERT INTO change_log(dataset, scope_key, op, row_key, payload_json, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, {NOW})"
            ),
            params![dataset, scope_key, op, row_key, payload_json],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn recent_changes(&self, after_seq: i64, limit: i64) -> Result<Vec<ChangeEntry>> {
        let conn = self.conn()?;
        let mut statement = conn.prepare(
            "SELECT seq, dataset, scope_key, op, row_key, payload_json, created_at \
             FROM change_log WHERE seq > ?1 ORDER BY seq LIMIT ?2",
        )?;
        let mut rows = statement.query(params![after_seq, limit])?;
        let mut changes = Vec::new();
        while let Some(row) = rows.next()? {
            let payload_json: String = row.get(5)?;
            changes.push(ChangeEntry {
                seq: row.get(0)?,
                dataset: row.get(1)?,
                scope_key: row.get(2)?,
                op: row.get(3)?,
                row_key: row.get(4)?,
                payload: serde_json::from_str(&payload_json)?,
                created_at: row.get(6)?,
            });
        }
        Ok(changes)
    }

    pub fn latest_change_seq(&self) -> Result<i64> {
        let conn = self.conn()?;
        Ok(
            conn.query_row("SELECT COALESCE(MAX(seq), 0) FROM change_log", [], |row| {
                row.get(0)
            })?,
        )
    }

    pub fn append_audit(
        &self,
        actor: &str,
        action: &str,
        target: &str,
        detail: &Value,
        ip: Option<&str>,
    ) -> Result<i64> {
        let detail_json = serde_json::to_string(detail)?;
        let conn = self.conn()?;
        conn.execute(
            &format!(
                "INSERT INTO audit_log(ts, actor, action, target, detail_json, ip) \
                 VALUES ({NOW}, ?1, ?2, ?3, ?4, ?5)"
            ),
            params![actor, action, target, detail_json, ip],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn recent_audit(&self, limit: i64) -> Result<Vec<AuditEntry>> {
        let conn = self.conn()?;
        let mut statement = conn.prepare(
            "SELECT id, ts, actor, action, target, detail_json, ip \
             FROM audit_log ORDER BY id DESC LIMIT ?1",
        )?;
        let mut rows = statement.query(params![limit])?;
        let mut entries = Vec::new();
        while let Some(row) = rows.next()? {
            let detail_json: String = row.get(5)?;
            entries.push(AuditEntry {
                id: row.get(0)?,
                ts: row.get(1)?,
                actor: row.get(2)?,
                action: row.get(3)?,
                target: row.get(4)?,
                detail: serde_json::from_str(&detail_json)?,
                ip: row.get(6)?,
            });
        }
        Ok(entries)
    }

    pub fn upsert_news_channel(
        &self,
        source: &str,
        channel_id: i64,
        name: &str,
        group_name: Option<&str>,
        enabled: bool,
    ) -> Result<()> {
        if source.trim().is_empty() || name.trim().is_empty() {
            return Err(StoreError::Invalid(
                "news channel source and name must not be empty".to_owned(),
            ));
        }
        let conn = self.conn()?;
        conn.execute(
            &format!(
                "INSERT INTO news_channels(source, channel_id, name, group_name, enabled, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, {NOW}) \
                 ON CONFLICT(source, channel_id) DO UPDATE SET name = excluded.name, \
                 group_name = excluded.group_name, enabled = excluded.enabled, \
                 updated_at = excluded.updated_at"
            ),
            params![source, channel_id, name, group_name, i64::from(enabled)],
        )?;
        Ok(())
    }

    pub fn list_news_channels(&self, enabled_only: bool) -> Result<Vec<NewsChannel>> {
        let conn = self.conn()?;
        let mut statement = conn.prepare(
            "SELECT source, channel_id, name, group_name, enabled, updated_at \
             FROM news_channels WHERE (?1 = 0 OR enabled = 1) \
             ORDER BY group_name, channel_id",
        )?;
        let mut rows = statement.query(params![i64::from(enabled_only)])?;
        let mut channels = Vec::new();
        while let Some(row) = rows.next()? {
            channels.push(NewsChannel {
                source: row.get(0)?,
                channel_id: row.get(1)?,
                name: row.get(2)?,
                group_name: row.get(3)?,
                enabled: row.get::<_, i64>(4)? != 0,
                updated_at: row.get(5)?,
            });
        }
        Ok(channels)
    }

    /// Insert or update a news item and append its change event atomically.
    pub fn upsert_news_article(&self, article: &NewsArticleInput) -> Result<i64> {
        validate_news_article(article)?;
        let mut conn = self.conn()?;
        let tx = conn.transaction()?;
        let existing = tx
            .query_row(
                "SELECT title, published_at, summary, info_src, url, attach_file, \
                 is_research, content_hash FROM news_articles \
                 WHERE source = ?1 AND scope_key = ?2 AND article_key = ?3",
                params![article.source, article.scope_key, article.article_key],
                |row| {
                    Ok(NewsSnapshot {
                        title: row.get(0)?,
                        published_at: row.get(1)?,
                        summary: row.get(2)?,
                        info_src: row.get(3)?,
                        url: row.get(4)?,
                        attach_file: row.get(5)?,
                        is_research: row.get::<_, i64>(6)? != 0,
                        content_hash: row.get(7)?,
                    })
                },
            )
            .optional()?;
        let changed = existing
            .as_ref()
            .map(|snapshot| news_changed(snapshot, article))
            .unwrap_or(true);

        tx.execute(
            &format!(
                "INSERT INTO news_articles(dataset, source, scope_type, scope_key, code, \
                 channel_id, kind, rec_id, article_key, title, published_at, summary, info_src, \
                 url, attach_file, is_research, content_hash, first_seen_at, last_seen_at, run_id) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, \
                 ?16, ?17, {NOW}, {NOW}, ?18) \
                 ON CONFLICT(source, scope_key, article_key) DO UPDATE SET \
                 dataset = excluded.dataset, scope_type = excluded.scope_type, \
                 code = excluded.code, channel_id = excluded.channel_id, kind = excluded.kind, \
                 rec_id = excluded.rec_id, title = excluded.title, \
                 published_at = excluded.published_at, summary = excluded.summary, \
                 info_src = excluded.info_src, url = excluded.url, \
                 attach_file = excluded.attach_file, is_research = excluded.is_research, \
                 content_hash = excluded.content_hash, last_seen_at = excluded.last_seen_at, \
                 run_id = excluded.run_id"
            ),
            params![
                article.dataset,
                article.source,
                article.scope_type,
                article.scope_key,
                article.code,
                article.channel_id,
                article.kind,
                article.rec_id,
                article.article_key,
                article.title,
                article.published_at,
                article.summary,
                article.info_src,
                article.url,
                article.attach_file,
                i64::from(article.is_research),
                article.content_hash,
                article.run_id,
            ],
        )?;

        let id: i64 = tx.query_row(
            "SELECT id FROM news_articles WHERE source = ?1 AND scope_key = ?2 AND article_key = ?3",
            params![article.source, article.scope_key, article.article_key],
            |row| row.get(0),
        )?;

        if changed {
            let payload = serde_json::json!({
                "id": id,
                "dataset": &article.dataset,
                "scope_key": &article.scope_key,
                "code": &article.code,
                "kind": &article.kind,
                "title": &article.title,
                "published_at": &article.published_at,
            });
            tx.execute(
                &format!(
                    "INSERT INTO change_log(dataset, scope_key, op, row_key, payload_json, created_at) \
                     VALUES (?1, ?2, 'upsert', ?3, ?4, {NOW})"
                ),
                params![
                    article.dataset,
                    article.scope_key,
                    id.to_string(),
                    serde_json::to_string(&payload)?
                ],
            )?;
        }
        tx.commit()?;
        Ok(id)
    }

    pub fn get_news_article_by_key(
        &self,
        source: &str,
        scope_key: &str,
        article_key: &str,
    ) -> Result<Option<NewsArticle>> {
        let conn = self.conn()?;
        let mut statement = conn.prepare(&format!(
            "SELECT {NEWS_COLUMNS} FROM news_articles \
             WHERE source = ?1 AND scope_key = ?2 AND article_key = ?3"
        ))?;
        let mut rows = statement.query(params![source, scope_key, article_key])?;
        match rows.next()? {
            Some(row) => Ok(Some(news_article_from_row(row)?)),
            None => Ok(None),
        }
    }

    /// Existing dedupe keys for one scope, used to stop pagination on a fully
    /// seen page without loading article bodies.
    pub fn list_news_keys(&self, source: &str, scope_key: &str) -> Result<Vec<String>> {
        let conn = self.conn()?;
        let mut statement = conn.prepare(
            "SELECT article_key FROM news_articles WHERE source = ?1 AND scope_key = ?2",
        )?;
        let mut rows = statement.query(params![source, scope_key])?;
        let mut keys = Vec::new();
        while let Some(row) = rows.next()? {
            keys.push(row.get(0)?);
        }
        Ok(keys)
    }

    pub fn list_news(
        &self,
        dataset: Option<&str>,
        scope_key: Option<&str>,
        limit: i64,
    ) -> Result<Vec<NewsArticle>> {
        let conn = self.conn()?;
        let mut statement = conn.prepare(&format!(
            "SELECT {NEWS_COLUMNS} FROM news_articles \
             WHERE (?1 IS NULL OR dataset = ?1) AND (?2 IS NULL OR scope_key = ?2) \
             ORDER BY COALESCE(published_at, first_seen_at) DESC, id DESC LIMIT ?3"
        ))?;
        let mut rows = statement.query(params![dataset, scope_key, limit.clamp(1, 1_000)])?;
        let mut articles = Vec::new();
        while let Some(row) = rows.next()? {
            articles.push(news_article_from_row(row)?);
        }
        Ok(articles)
    }

    pub fn count_news_articles(&self) -> Result<i64> {
        let conn = self.conn()?;
        Ok(conn.query_row("SELECT COUNT(*) FROM news_articles", [], |row| row.get(0))?)
    }

    pub fn upsert_news_body(
        &self,
        article_id: i64,
        detail_type: &str,
        content: &str,
        content_hash: Option<&str>,
        run_id: Option<i64>,
    ) -> Result<()> {
        if detail_type.trim().is_empty() {
            return Err(StoreError::Invalid(
                "news body detail_type must not be empty".to_owned(),
            ));
        }
        let conn = self.conn()?;
        conn.execute(
            &format!(
                "INSERT INTO news_bodies(article_id, detail_type, content, content_hash, \
                 fetched_at, run_id) VALUES (?1, ?2, ?3, ?4, {NOW}, ?5) \
                 ON CONFLICT(article_id, detail_type) DO UPDATE SET content = excluded.content, \
                 content_hash = excluded.content_hash, fetched_at = excluded.fetched_at, \
                 run_id = excluded.run_id"
            ),
            params![article_id, detail_type, content, content_hash, run_id],
        )?;
        Ok(())
    }

    pub fn get_news_body(&self, article_id: i64, detail_type: &str) -> Result<Option<NewsBody>> {
        let conn = self.conn()?;
        let body = conn
            .query_row(
                "SELECT article_id, detail_type, content, content_hash, fetched_at, run_id \
                 FROM news_bodies WHERE article_id = ?1 AND detail_type = ?2",
                params![article_id, detail_type],
                |row| {
                    Ok(NewsBody {
                        article_id: row.get(0)?,
                        detail_type: row.get(1)?,
                        content: row.get(2)?,
                        content_hash: row.get(3)?,
                        fetched_at: row.get(4)?,
                        run_id: row.get(5)?,
                    })
                },
            )
            .optional()?;
        Ok(body)
    }

    pub fn count_news_bodies(&self) -> Result<i64> {
        let conn = self.conn()?;
        Ok(conn.query_row("SELECT COUNT(*) FROM news_bodies", [], |row| row.get(0))?)
    }

    pub fn prune_news_before(&self, before: &str) -> Result<usize> {
        let conn = self.conn()?;
        Ok(conn.execute(
            "DELETE FROM news_articles WHERE last_seen_at < ?1",
            params![before],
        )?)
    }

    pub fn prune_news_bodies_before(&self, before: &str) -> Result<usize> {
        let conn = self.conn()?;
        Ok(conn.execute(
            "DELETE FROM news_bodies WHERE fetched_at < ?1",
            params![before],
        )?)
    }

    pub fn prune_payloads_before(&self, before: &str) -> Result<usize> {
        let conn = self.conn()?;
        Ok(conn.execute(
            "DELETE FROM collected_payloads WHERE fetched_at < ?1",
            params![before],
        )?)
    }

    pub fn prune_change_log_keep(&self, keep: i64) -> Result<usize> {
        let conn = self.conn()?;
        Ok(conn.execute(
            "DELETE FROM change_log WHERE seq NOT IN \
             (SELECT seq FROM change_log ORDER BY seq DESC LIMIT ?1)",
            params![keep.max(0)],
        )?)
    }

    pub fn prune_audit_before(&self, before: &str) -> Result<usize> {
        let conn = self.conn()?;
        Ok(conn.execute("DELETE FROM audit_log WHERE ts < ?1", params![before])?)
    }

    pub fn incremental_vacuum(&self) -> Result<()> {
        let conn = self.conn()?;
        conn.execute_batch("PRAGMA incremental_vacuum;")?;
        Ok(())
    }
}

fn configure(conn: &Connection) -> Result<()> {
    conn.busy_timeout(Duration::from_secs(5))?;
    conn.execute_batch(
        "PRAGMA foreign_keys = ON; \
         PRAGMA synchronous = NORMAL; \
         PRAGMA auto_vacuum = INCREMENTAL;",
    )?;
    let _: String = conn.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
    Ok(())
}

fn hash_password(password: &str) -> Result<String> {
    Argon2::default()
        .hash_password(password.as_bytes())
        .map(|hash| hash.to_string())
        .map_err(|error| StoreError::Password(error.to_string()))
}

fn verify_password(password: &str, hash: &str) -> Result<bool> {
    let parsed =
        PasswordHash::new(hash).map_err(|error| StoreError::Password(error.to_string()))?;
    Ok(Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok())
}

fn validate_new_task(task: &NewTask) -> Result<()> {
    if task.name.trim().is_empty() {
        return Err(StoreError::Invalid(
            "task name must not be empty".to_owned(),
        ));
    }
    if task.dataset.trim().is_empty() {
        return Err(StoreError::Invalid(
            "task dataset must not be empty".to_owned(),
        ));
    }
    if task.method.trim().is_empty() {
        return Err(StoreError::Invalid(
            "task method must not be empty".to_owned(),
        ));
    }
    if task.port == 0 {
        return Err(StoreError::Invalid("task port must be non-zero".to_owned()));
    }
    if task.shard_size <= 0 {
        return Err(StoreError::Invalid(
            "task shard_size must be positive".to_owned(),
        ));
    }
    if task.concurrency <= 0 {
        return Err(StoreError::Invalid(
            "task concurrency must be positive".to_owned(),
        ));
    }
    Ok(())
}

fn task_from_row(row: &Row<'_>) -> Result<CollectionTask> {
    let params_json: String = row.get(5)?;
    let mode: String = row.get(6)?;
    Ok(CollectionTask {
        id: row.get(0)?,
        name: row.get(1)?,
        port: row.get::<_, i64>(2)? as u16,
        dataset: row.get(3)?,
        method: row.get(4)?,
        params: serde_json::from_str(&params_json)?,
        mode: CollectionMode::from_str(&mode)?,
        watermark_type: row.get(7)?,
        overlap_days: row.get(8)?,
        shard_size: row.get(9)?,
        max_pages: row.get(10)?,
        max_records: row.get(11)?,
        schedule_kind: row.get(12)?,
        schedule_expr: row.get(13)?,
        enabled: row.get::<_, i64>(14)? != 0,
        timeout_ms: row.get(15)?,
        retry: row.get(16)?,
        concurrency: row.get(17)?,
        retention_days: row.get(18)?,
        created_at: row.get(19)?,
        updated_at: row.get(20)?,
    })
}

fn news_article_from_row(row: &Row<'_>) -> Result<NewsArticle> {
    Ok(NewsArticle {
        id: row.get(0)?,
        dataset: row.get(1)?,
        source: row.get(2)?,
        scope_type: row.get(3)?,
        scope_key: row.get(4)?,
        code: row.get(5)?,
        channel_id: row.get(6)?,
        kind: row.get(7)?,
        rec_id: row.get(8)?,
        article_key: row.get(9)?,
        title: row.get(10)?,
        published_at: row.get(11)?,
        summary: row.get(12)?,
        info_src: row.get(13)?,
        url: row.get(14)?,
        attach_file: row.get(15)?,
        is_research: row.get::<_, i64>(16)? != 0,
        content_hash: row.get(17)?,
        first_seen_at: row.get(18)?,
        last_seen_at: row.get(19)?,
        run_id: row.get(20)?,
    })
}

struct NewsSnapshot {
    title: String,
    published_at: Option<String>,
    summary: Option<String>,
    info_src: Option<String>,
    url: Option<String>,
    attach_file: Option<String>,
    is_research: bool,
    content_hash: Option<String>,
}

fn news_changed(snapshot: &NewsSnapshot, article: &NewsArticleInput) -> bool {
    snapshot.title != article.title
        || snapshot.published_at != article.published_at
        || snapshot.summary != article.summary
        || snapshot.info_src != article.info_src
        || snapshot.url != article.url
        || snapshot.attach_file != article.attach_file
        || snapshot.is_research != article.is_research
        || snapshot.content_hash != article.content_hash
}

fn validate_news_article(article: &NewsArticleInput) -> Result<()> {
    if !article.dataset.starts_with("news.") {
        return Err(StoreError::Invalid(
            "news dataset must start with news.".to_owned(),
        ));
    }
    for (value, label) in [
        (&article.source, "source"),
        (&article.scope_type, "scope_type"),
        (&article.scope_key, "scope_key"),
        (&article.kind, "kind"),
        (&article.rec_id, "rec_id"),
        (&article.article_key, "article_key"),
        (&article.title, "title"),
    ] {
        if value.trim().is_empty() {
            return Err(StoreError::Invalid(format!(
                "news {label} must not be empty"
            )));
        }
    }
    if article.channel_id.is_some_and(|id| id <= 0) {
        return Err(StoreError::Invalid(
            "news channel_id must be positive".to_owned(),
        ));
    }
    Ok(())
}
