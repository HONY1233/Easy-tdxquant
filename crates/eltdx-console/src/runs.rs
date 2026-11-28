//! In-process bookkeeping for active runs.
//!
//! The store is the source of truth for run rows; this registry only prevents
//! two schedulers/operators from starting the same task twice and carries the
//! cooperative cancellation flag into the engine.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Default)]
pub struct RunRegistry {
    tasks: Mutex<HashSet<i64>>,
    cancels: Mutex<HashMap<i64, Arc<AtomicBool>>>,
}

impl RunRegistry {
    /// Claims a task for execution; `false` means it is already running.
    pub fn claim_task(&self, task_id: i64) -> bool {
        self.tasks
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(task_id)
    }

    pub fn release_task(&self, task_id: i64) {
        self.tasks
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&task_id);
    }

    pub fn is_task_running(&self, task_id: i64) -> bool {
        self.tasks
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .contains(&task_id)
    }

    /// Creates (or reuses) the cancellation flag for a run.
    pub fn register_run(&self, run_id: i64) -> Arc<AtomicBool> {
        let mut cancels = self
            .cancels
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        Arc::clone(
            cancels
                .entry(run_id)
                .or_insert_with(|| Arc::new(AtomicBool::new(false))),
        )
    }

    /// Requests cancellation; `false` when the run is not active here.
    pub fn cancel_run(&self, run_id: i64) -> bool {
        let cancels = self
            .cancels
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        match cancels.get(&run_id) {
            Some(flag) => {
                flag.store(true, Ordering::SeqCst);
                true
            }
            None => false,
        }
    }

    pub fn finish_run(&self, run_id: i64) {
        self.cancels
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&run_id);
    }

    pub fn active_runs(&self) -> Vec<i64> {
        let mut runs: Vec<i64> = self
            .cancels
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .keys()
            .copied()
            .collect();
        runs.sort_unstable();
        runs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tasks_are_claimed_once_at_a_time() {
        let registry = RunRegistry::default();
        assert!(registry.claim_task(7));
        assert!(!registry.claim_task(7));
        assert!(registry.is_task_running(7));
        registry.release_task(7);
        assert!(!registry.is_task_running(7));
        assert!(registry.claim_task(7));
    }

    #[test]
    fn cancellation_is_per_run_and_idempotent() {
        let registry = RunRegistry::default();
        let flag = registry.register_run(11);
        assert_eq!(registry.active_runs(), vec![11]);
        assert!(!flag.load(Ordering::SeqCst));
        assert!(registry.cancel_run(11));
        assert!(flag.load(Ordering::SeqCst));
        assert!(registry.cancel_run(11));
        assert!(!registry.cancel_run(12));
        registry.finish_run(11);
        assert!(registry.active_runs().is_empty());
        assert!(!registry.cancel_run(11));
    }
}
