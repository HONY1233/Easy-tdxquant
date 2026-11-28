//! Task execution: shard a collection task across codes, call the data plane
//! with per-request timeouts and retries, persist every payload, and close the
//! run with checkpoints plus dataset state.
//!
//! Scope of this slice: one gateway call per shard. Paging stays with the
//! dataset-specific collectors (news already has one); generic methods that
//! page must be modelled explicitly rather than guessed from a response shape,
//! so nothing here silently drops later pages.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use eltdx_store::{CollectionMode, CollectionTask, RunStatus, Store, StoreError};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::clock::{format_iso_utc, now_epoch, shanghai_date as shanghai_date_at};

/// Boxed RPC future so the engine can stay object-safe and spawnable.
pub type RpcFuture<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

/// Async boundary onto the local gateway. Implemented for `GatewayHandle` in
/// production and by deterministic mocks in tests.
pub trait Rpc: Send + Sync {
    fn call(&self, method: &str, params: Value) -> RpcFuture<'_>;
}

#[derive(Debug, Error)]
pub enum CollectError {
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("task {0} does not exist")]
    TaskNotFound(i64),
    #[error("task {task_id} is disabled")]
    TaskDisabled { task_id: i64 },
    #[error("task {task_id} has no gateway method")]
    MissingMethod { task_id: i64 },
}

#[derive(Debug, Clone)]
pub struct RunOptions {
    pub trigger: String,
    pub resumed_from: Option<i64>,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            trigger: "manual".to_owned(),
            resumed_from: None,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct RunReport {
    pub run_id: i64,
    pub task_id: i64,
    pub status: String,
    pub shards: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub cancelled: bool,
    pub rows: i64,
    pub bytes: i64,
    pub retries: u32,
    pub duration_ms: u64,
    pub error: Option<String>,
}

struct ShardOutcome {
    rows: i64,
    bytes: i64,
    retries: u32,
}

/// Splits `codes` into fixed-size shards; anything else is a single unit.
pub fn build_shards(task: &CollectionTask) -> Vec<(String, Value)> {
    let codes = task
        .params
        .get("codes")
        .and_then(Value::as_array)
        .filter(|codes| !codes.is_empty());
    let Some(codes) = codes else {
        return vec![("all".to_owned(), task.params.clone())];
    };
    let size = usize::try_from(task.shard_size.max(1)).unwrap_or(1);
    codes
        .chunks(size.max(1))
        .map(|chunk| {
            let mut params = task.params.clone();
            if let Some(map) = params.as_object_mut() {
                map.insert("codes".to_owned(), Value::Array(chunk.to_vec()));
            }
            let first = chunk
                .first()
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_owned();
            (format!("codes:{first}"), params)
        })
        .collect()
}

/// Rows carried by a gateway result: the array length, a known envelope's
/// array, or one scalar record.
pub fn count_rows(value: &Value) -> i64 {
    match value {
        Value::Null => 0,
        Value::Array(items) => items.len() as i64,
        Value::Object(map) => {
            for key in ["rows", "items", "records", "bars", "data", "list"] {
                if let Some(Value::Array(items)) = map.get(key) {
                    return items.len() as i64;
                }
            }
            if map.is_empty() {
                0
            } else {
                1
            }
        }
        _ => 1,
    }
}

/// Opens a run row before any network work so the console can show progress.
pub async fn start_run(
    store: &Arc<Store>,
    task: &CollectionTask,
    options: &RunOptions,
) -> Result<i64, CollectError> {
    let store = Arc::clone(store);
    let task = task.clone();
    let trigger = options.trigger.clone();
    let resumed_from = options.resumed_from;
    let run_id = tokio::task::spawn_blocking(move || {
        store.start_run(task.id, &trigger, task.mode, None, None, None, resumed_from)
    })
    .await
    .map_err(|error| StoreError::Invalid(error.to_string()))??;
    Ok(run_id)
}

/// Runs an already-open run row to completion without a cancellation handle.
pub async fn execute_run<R: Rpc + Clone + 'static>(
    store: Arc<Store>,
    rpc: R,
    task: CollectionTask,
    run_id: i64,
) -> Result<RunReport, CollectError> {
    execute_run_cancellable(store, rpc, task, run_id, Arc::new(AtomicBool::new(false))).await
}

/// Runs an already-open run row, honouring a cooperative cancellation flag.
///
/// The flag is checked between shard batches, so in-flight requests finish and
/// their payloads are kept; the run is then closed as `cancelled`.
pub async fn execute_run_cancellable<R: Rpc + Clone + 'static>(
    store: Arc<Store>,
    rpc: R,
    task: CollectionTask,
    run_id: i64,
    cancel: Arc<AtomicBool>,
) -> Result<RunReport, CollectError> {
    if task.method.trim().is_empty() {
        let error = CollectError::MissingMethod { task_id: task.id };
        finish(
            &store,
            run_id,
            RunStatus::Failed,
            0,
            0,
            Some(&error.to_string()),
        )
        .await?;
        return Err(error);
    }

    let started = Instant::now();
    let shards = build_shards(&task);
    let concurrency = usize::try_from(task.concurrency.max(1)).unwrap_or(1);
    let mut succeeded = 0_usize;
    let mut failed = 0_usize;
    let mut rows = 0_i64;
    let mut bytes = 0_i64;
    let mut retries = 0_u32;
    let mut first_error: Option<String> = None;
    let mut cancelled = false;

    for batch in shards.chunks(concurrency) {
        if cancel.load(Ordering::SeqCst) {
            cancelled = true;
            break;
        }
        let mut set = tokio::task::JoinSet::new();
        for (key, params) in batch {
            let store = Arc::clone(&store);
            let rpc = rpc.clone();
            let task = task.clone();
            let key = key.clone();
            let params = params.clone();
            set.spawn(async move { run_shard(&store, &rpc, &task, run_id, &key, params).await });
        }
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok(Ok(outcome)) => {
                    succeeded += 1;
                    rows += outcome.rows;
                    bytes += outcome.bytes;
                    retries += outcome.retries;
                }
                Ok(Err(message)) => {
                    failed += 1;
                    if first_error.is_none() {
                        first_error = Some(message);
                    }
                }
                Err(error) => {
                    failed += 1;
                    if first_error.is_none() {
                        first_error = Some(format!("shard task failed: {error}"));
                    }
                }
            }
        }
    }

    let status = if cancelled {
        RunStatus::Cancelled
    } else if failed == 0 {
        RunStatus::Success
    } else {
        // Partial collection is reported as a failure: the dataset is
        // incomplete and must not look like a clean run.
        RunStatus::Failed
    };
    let error = if cancelled {
        Some("cancelled before the remaining shards ran".to_owned())
    } else {
        first_error.map(|message| {
            if failed > 1 {
                format!(
                    "{failed}/{} shards failed; first error: {message}",
                    shards.len()
                )
            } else {
                message
            }
        })
    };

    let store_write = Arc::clone(&store);
    let dataset = task.dataset.clone();
    let port = task.port;
    let mode = task.mode;
    let shard_total = shards.len();
    let success = succeeded;
    let error_for_run = error.clone();
    tokio::task::spawn_blocking(move || -> Result<(), StoreError> {
        store_write.finish_run(run_id, status, rows, bytes, error_for_run.as_deref(), None)?;
        if failed == 0 && !cancelled {
            if mode == CollectionMode::Full {
                store_write.mark_full(&dataset, &format!("port:{port}"), run_id, rows)?;
            } else {
                let state = eltdx_store::DatasetState {
                    dataset: dataset.clone(),
                    scope_key: format!("port:{port}"),
                    watermark: None,
                    last_full_at: None,
                    last_incremental_at: Some(now_iso()),
                    last_run_id: Some(run_id),
                    status: "success".to_owned(),
                    rows,
                    updated_at: now_iso(),
                };
                store_write.upsert_dataset_state(&state)?;
            }
        }
        Ok(())
    })
    .await
    .map_err(|error| StoreError::Invalid(error.to_string()))??;

    Ok(RunReport {
        run_id,
        task_id: task.id,
        status: status.as_str().to_owned(),
        shards: shard_total,
        succeeded: success,
        failed,
        cancelled,
        rows,
        bytes,
        retries,
        duration_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        error,
    })
}

async fn run_shard<R: Rpc>(
    store: &Arc<Store>,
    rpc: &R,
    task: &CollectionTask,
    run_id: i64,
    shard_key: &str,
    params: Value,
) -> Result<ShardOutcome, String> {
    let timeout = Duration::from_millis(u64::try_from(task.timeout_ms.max(1)).unwrap_or(8000));
    let attempts = u32::try_from(task.retry.max(0))
        .unwrap_or(0)
        .saturating_add(1);
    let mut last_error = String::new();

    for attempt in 0..attempts {
        let call = rpc.call(&task.method, params.clone());
        match tokio::time::timeout(timeout, call).await {
            Ok(Ok(value)) => {
                let outcome = persist(store, task, run_id, shard_key, &params, &value).await?;
                return Ok(ShardOutcome {
                    retries: attempt,
                    ..outcome
                });
            }
            Ok(Err(message)) => last_error = message,
            Err(_) => last_error = format!("timed out after {} ms", timeout.as_millis()),
        }
        if attempt + 1 < attempts {
            tokio::time::sleep(Duration::from_millis(250 * u64::from(attempt + 1))).await;
        }
    }

    let store = Arc::clone(store);
    let shard_key = shard_key.to_owned();
    let message = last_error.clone();
    let _ = tokio::task::spawn_blocking(move || {
        store.save_checkpoint(
            run_id,
            &shard_key,
            &json!({ "error": message }),
            "failed",
            0,
        )
    })
    .await;
    Err(last_error)
}

async fn persist(
    store: &Arc<Store>,
    task: &CollectionTask,
    run_id: i64,
    shard_key: &str,
    params: &Value,
    value: &Value,
) -> Result<ShardOutcome, String> {
    let rows = count_rows(value);
    let bytes = serde_json::to_vec(value)
        .map_err(|error| error.to_string())?
        .len() as i64;
    let params_hash = hash_params(params);
    let code = params
        .get("codes")
        .and_then(Value::as_array)
        .and_then(|codes| codes.first())
        .and_then(Value::as_str)
        .unwrap_or(shard_key)
        .to_owned();
    let trade_date = shanghai_date();

    let store = Arc::clone(store);
    let dataset = task.dataset.clone();
    let port = task.port;
    let payload = value.clone();
    let shard = shard_key.to_owned();
    let hash = params_hash.clone();
    tokio::task::spawn_blocking(move || -> Result<(), StoreError> {
        store.upsert_payload(
            Some(run_id),
            port,
            &dataset,
            &code,
            &trade_date,
            &hash,
            &payload,
        )?;
        store.save_checkpoint(
            run_id,
            &shard,
            &json!({ "params_hash": hash, "rows": rows, "bytes": bytes }),
            "success",
            rows,
        )
    })
    .await
    .map_err(|error| error.to_string())?
    .map_err(|error| error.to_string())?;

    Ok(ShardOutcome {
        rows,
        bytes,
        retries: 0,
    })
}

async fn finish(
    store: &Arc<Store>,
    run_id: i64,
    status: RunStatus,
    rows: i64,
    bytes: i64,
    error: Option<&str>,
) -> Result<(), CollectError> {
    let store = Arc::clone(store);
    let error = error.map(str::to_owned);
    tokio::task::spawn_blocking(move || {
        store.finish_run(run_id, status, rows, bytes, error.as_deref(), None)
    })
    .await
    .map_err(|error| StoreError::Invalid(error.to_string()))??;
    Ok(())
}

/// Loads a task, opens a run and executes it in one call (CLI and tests).
pub async fn run_task<R: Rpc + Clone + 'static>(
    store: Arc<Store>,
    rpc: R,
    task_id: i64,
    options: RunOptions,
) -> Result<RunReport, CollectError> {
    let task = {
        let store = Arc::clone(&store);
        tokio::task::spawn_blocking(move || store.get_task(task_id))
            .await
            .map_err(|error| StoreError::Invalid(error.to_string()))??
    };
    let task = task.ok_or(CollectError::TaskNotFound(task_id))?;
    if !task.enabled {
        return Err(CollectError::TaskDisabled { task_id });
    }
    let run_id = start_run(&store, &task, &options).await?;
    execute_run(store, rpc, task, run_id).await
}

fn hash_params(params: &Value) -> String {
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_vec(params).unwrap_or_default());
    let digest = hasher.finalize();
    let mut out = String::with_capacity(64);
    for byte in digest {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// Current Shanghai date (`YYYY-MM-DD`), used as the payload partition key.
fn shanghai_date() -> String {
    shanghai_date_at(now_epoch())
}

fn now_iso() -> String {
    format_iso_utc(now_epoch())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use eltdx_store::{CollectionMode, NewTask};
    use serde_json::json;

    use super::*;
    use crate::clock::civil_from_days;

    fn store() -> Arc<Store> {
        match Store::open_in_memory() {
            Ok(store) => Arc::new(store),
            Err(error) => panic!("open store: {error}"),
        }
    }

    fn task(params: Value) -> NewTask {
        NewTask::new(
            "engine test",
            7709,
            "quote.snapshot",
            "quotes.get_snapshots",
            params,
            CollectionMode::Incremental,
        )
    }

    #[derive(Clone)]
    struct MockRpc {
        calls: Arc<AtomicU32>,
        fail_first: Arc<AtomicU32>,
    }

    impl MockRpc {
        fn new(fail_first: u32) -> Self {
            Self {
                calls: Arc::new(AtomicU32::new(0)),
                fail_first: Arc::new(AtomicU32::new(fail_first)),
            }
        }
    }

    impl Rpc for MockRpc {
        fn call(&self, _method: &str, params: Value) -> RpcFuture<'_> {
            let index = self.calls.fetch_add(1, Ordering::SeqCst);
            let fail_first = self.fail_first.load(Ordering::SeqCst);
            let count = params
                .get("codes")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or(1);
            Box::pin(async move {
                if index < fail_first {
                    return Err("synthetic transport failure".to_owned());
                }
                Ok(Value::Array(
                    (0..count)
                        .map(|index| json!({ "ok": true, "index": index }))
                        .collect(),
                ))
            })
        }
    }

    #[test]
    fn shards_split_codes_and_keep_stable_keys() {
        let mut task = task(json!({"codes": ["sz000001", "sz000002", "sh600000"]}));
        task.shard_size = 2;
        let created = {
            let store = store();
            let id = match store.create_task(&task) {
                Ok(id) => id,
                Err(error) => panic!("create task: {error}"),
            };
            match store.get_task(id) {
                Ok(Some(task)) => task,
                _ => panic!("task must exist"),
            }
        };
        let shards = build_shards(&created);
        assert_eq!(shards.len(), 2);
        assert_eq!(shards[0].0, "codes:sz000001");
        assert_eq!(shards[1].0, "codes:sh600000");
        assert_eq!(shards[0].1["codes"].as_array().map(Vec::len), Some(2));
    }

    #[test]
    fn rows_are_counted_for_arrays_and_envelopes() {
        assert_eq!(count_rows(&Value::Null), 0);
        assert_eq!(count_rows(&json!([])), 0);
        assert_eq!(count_rows(&json!([1, 2, 3])), 3);
        assert_eq!(count_rows(&json!({ "rows": [1, 2] })), 2);
        assert_eq!(count_rows(&json!({ "items": [] })), 0);
        assert_eq!(count_rows(&json!({ "last_price": 12.3 })), 1);
        assert_eq!(count_rows(&json!({})), 0);
    }

    #[test]
    fn shanghai_date_matches_known_days() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(20_000), (2024, 10, 4));
        let today = shanghai_date();
        assert_eq!(today.len(), 10);
        assert!(today.starts_with("20"));
    }

    #[tokio::test]
    async fn run_persists_payloads_retries_and_dataset_state() {
        let store = store();
        let mut new_task = task(json!({"codes": ["sz000001", "sz000002", "sh600000"]}));
        new_task.shard_size = 2;
        new_task.retry = 1;
        let id = match store.create_task(&new_task) {
            Ok(id) => id,
            Err(error) => panic!("create task: {error}"),
        };

        let rpc = MockRpc::new(1);
        let report = match run_task(Arc::clone(&store), rpc, id, RunOptions::default()).await {
            Ok(report) => report,
            Err(error) => panic!("run failed: {error}"),
        };
        assert_eq!(report.status, "success");
        assert_eq!(report.shards, 2);
        assert_eq!(report.succeeded, 2);
        assert_eq!(report.failed, 0);
        assert_eq!(report.rows, 3);
        assert_eq!(report.retries, 1);
        assert_eq!(store.count_payloads().unwrap_or(-1), 2);
        let checkpoints = store.list_checkpoints(report.run_id).unwrap_or_default();
        assert_eq!(checkpoints.len(), 2);
        assert!(checkpoints.iter().all(|point| point.status == "success"));
        let state = store
            .get_dataset_state("quote.snapshot", "port:7709")
            .unwrap_or(None);
        assert_eq!(state.map(|state| state.rows), Some(3));
        let runs = store.recent_runs(5).unwrap_or_default();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].status, "success");
    }

    #[tokio::test]
    async fn failing_shards_mark_the_run_failed() {
        let store = store();
        let new_task = task(json!({"codes": ["sz000001"]}));
        let id = match store.create_task(&new_task) {
            Ok(id) => id,
            Err(error) => panic!("create task: {error}"),
        };
        let rpc = MockRpc::new(99);
        let report = match run_task(Arc::clone(&store), rpc, id, RunOptions::default()).await {
            Ok(report) => report,
            Err(error) => panic!("run failed unexpectedly: {error}"),
        };
        assert_eq!(report.status, "failed");
        assert_eq!(report.failed, 1);
        assert_eq!(report.rows, 0);
        assert!(report.error.is_some());
        let checkpoints = store.list_checkpoints(report.run_id).unwrap_or_default();
        assert_eq!(checkpoints.len(), 1);
        assert_eq!(checkpoints[0].status, "failed");
    }

    #[tokio::test]
    async fn cancellation_closes_the_run_without_starting_shards() {
        let store = store();
        let mut new_task = task(json!({"codes": ["sz000001", "sz000002"]}));
        new_task.shard_size = 1;
        let id = match store.create_task(&new_task) {
            Ok(id) => id,
            Err(error) => panic!("create task: {error}"),
        };
        let loaded = match store.get_task(id) {
            Ok(Some(task)) => task,
            _ => panic!("task must exist"),
        };
        let run_id = match start_run(&store, &loaded, &RunOptions::default()).await {
            Ok(run_id) => run_id,
            Err(error) => panic!("start run: {error}"),
        };
        let flag = Arc::new(AtomicBool::new(true));
        let report = match execute_run_cancellable(
            Arc::clone(&store),
            MockRpc::new(0),
            loaded,
            run_id,
            flag,
        )
        .await
        {
            Ok(report) => report,
            Err(error) => panic!("run failed: {error}"),
        };
        assert_eq!(report.status, "cancelled");
        assert!(report.cancelled);
        assert_eq!(report.rows, 0);
        assert_eq!(report.succeeded, 0);
        assert_eq!(store.count_payloads().unwrap_or(-1), 0);
        let runs = store.recent_runs(5).unwrap_or_default();
        assert_eq!(
            runs.first().map(|run| run.status.as_str()),
            Some("cancelled")
        );
    }

    #[tokio::test]
    async fn disabled_tasks_are_not_run() {
        let store = store();
        let new_task = task(json!({}));
        let id = match store.create_task(&new_task) {
            Ok(id) => id,
            Err(error) => panic!("create task: {error}"),
        };
        if let Err(error) = store.set_task_enabled(id, false) {
            panic!("disable task: {error}");
        }
        match run_task(
            Arc::clone(&store),
            MockRpc::new(0),
            id,
            RunOptions::default(),
        )
        .await
        {
            Ok(_) => panic!("disabled task must not run"),
            Err(error) => assert!(matches!(error, CollectError::TaskDisabled { .. })),
        }
    }
}
