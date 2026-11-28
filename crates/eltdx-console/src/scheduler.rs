//! Background scheduler: starts tasks whose schedule is due and runs the
//! retention sweep on a fixed cadence.
//!
//! The loop is deliberately single-node and idempotent: a task is only started
//! after the in-process registry claims it, so an overlapping tick cannot
//! double-run the same task.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use crate::clock::now_epoch;
use crate::collect::{execute_run_cancellable, start_run, RunOptions};
use crate::retention::{prune, RetentionPolicy};
use crate::schedule::{is_due, Schedule};
use crate::server::ConsoleState;

const TICK: Duration = Duration::from_secs(30);
const PRUNE_EVERY_SECONDS: i64 = 6 * 3600;

/// Tick interval; `ELTDX_CONSOLE_TICK_SECONDS` overrides it for tests and
/// tuning, clamped to 1..=3600 seconds.
fn tick_interval() -> Duration {
    let seconds = std::env::var("ELTDX_CONSOLE_TICK_SECONDS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(TICK.as_secs())
        .clamp(1, 3600);
    Duration::from_secs(seconds)
}

pub fn spawn(state: Arc<ConsoleState>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(tick_interval());
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_prune = 0_i64;
        let mut reported: HashSet<i64> = HashSet::new();
        loop {
            ticker.tick().await;
            scheduled_tick(&state, &mut reported).await;
            let now = now_epoch();
            if now.saturating_sub(last_prune) >= PRUNE_EVERY_SECONDS {
                last_prune = now;
                prune_sweep(&state).await;
            }
        }
    })
}

/// One scheduler pass; exposed for tests. `reported` de-duplicates invalid
/// schedule audit entries so a bad expression is logged once per process.
pub async fn scheduled_tick(state: &Arc<ConsoleState>, reported: &mut HashSet<i64>) {
    let store = state.store_handle();
    let tasks = match tokio::task::spawn_blocking(move || store.list_tasks()).await {
        Ok(Ok(tasks)) => tasks,
        _ => return,
    };
    for task in tasks {
        if !task.enabled {
            continue;
        }
        let schedule = match Schedule::parse(&task.schedule_kind, task.schedule_expr.as_deref()) {
            Ok(schedule) => schedule,
            Err(error) => {
                // An invalid expression must be visible, not silently skipped,
                // but only once per task per process.
                if reported.insert(task.id) {
                    let store = state.store_handle();
                    let target = format!("collection_tasks/{}", task.id);
                    let message = error.to_string();
                    let _ = tokio::task::spawn_blocking(move || {
                        store.append_audit(
                            "system",
                            "schedule.invalid",
                            &target,
                            &json!({ "error": message }),
                            None,
                        )
                    })
                    .await;
                }
                continue;
            }
        };
        if schedule == Schedule::Manual {
            continue;
        }
        if state.runs().is_task_running(task.id) {
            continue;
        }
        let store = state.store_handle();
        let task_id = task.id;
        let last =
            match tokio::task::spawn_blocking(move || store.latest_run_started_at(task_id)).await {
                Ok(Ok(last)) => last,
                _ => None,
            };
        if !is_due(&schedule, last.as_deref(), now_epoch()) {
            continue;
        }
        if !state.runs().claim_task(task.id) {
            continue;
        }
        let store = state.store_handle();
        let run_id = match start_run(
            &store,
            &task,
            &RunOptions {
                trigger: "schedule".to_owned(),
                resumed_from: None,
            },
        )
        .await
        {
            Ok(run_id) => run_id,
            Err(_) => {
                state.runs().release_task(task.id);
                continue;
            }
        };
        let flag = state.runs().register_run(run_id);
        let background = Arc::clone(state);
        let pending = task.clone();
        tokio::spawn(async move {
            let store = Arc::clone(&background.store_handle());
            let gateway = background.gateway();
            let _ = execute_run_cancellable(store, gateway, pending.clone(), run_id, flag).await;
            background.runs().finish_run(run_id);
            background.runs().release_task(pending.id);
        });
    }
}

async fn prune_sweep(state: &Arc<ConsoleState>) {
    let store = state.store_handle();
    let policy =
        match tokio::task::spawn_blocking(move || RetentionPolicy::from_settings(&store)).await {
            Ok(Ok(policy)) => policy,
            _ => return,
        };
    let Ok(report) = prune(&state.store_handle(), policy).await else {
        return;
    };
    let store = state.store_handle();
    let _ = tokio::task::spawn_blocking(move || {
        store.append_audit(
            "system",
            "maintenance.prune",
            "retention",
            &json!({ "report": report, "mode": "scheduled" }),
            None,
        )
    })
    .await;
}
