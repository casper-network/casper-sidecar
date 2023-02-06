use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::Notify;
/// An object to support synchronizing connections to multiple filters.
///
/// This should be used when support for partial connections is disallowed.  In that case, a
/// failure to connect to any filter should cause all connections to fail without reading any events
/// from the stream(s).
#[derive(Clone)]
pub(super) struct ConnectionTasks {
    /// The total number filters to which the sidecar is attempting to connect.
    total: usize,
    /// The number of filters to which successful connections have been established.
    successes: Arc<AtomicUsize>,
    /// Whether the sidecar has given up trying to connect to any filter.
    failure: Arc<AtomicBool>,
    /// A notifier to wake connection tasks before checking whether an overall result has been
    /// reached.
    notify: Arc<Notify>,
}

impl ConnectionTasks {
    /// Returns a new `ConnectionTasks` where a clone of this instance is expected to be passed to
    /// `total` different connection tasks.
    pub(super) fn new(total: usize) -> Self {
        ConnectionTasks {
            total,
            successes: Arc::new(AtomicUsize::new(0)),
            failure: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Increments the `successes` counter and notifies other tasks currently awaiting an overall
    /// result if we now have one.
    pub(super) fn register_success(&self) {
        let successes = self.successes.fetch_add(1, Ordering::SeqCst) + 1;
        if successes >= self.total || self.failure.load(Ordering::SeqCst) {
            self.notify.notify_one();
        }
    }

    /// Sets `failure` to true and notifies other tasks currently awaiting an overall result.
    pub(super) fn register_failure(&self) {
        self.failure.store(true, Ordering::SeqCst);
        self.notify.notify_one();
    }

    /// Waits for an overall result and returns true when `successes == total` or false if
    /// `failure` is set.
    pub(super) async fn wait(&self) -> bool {
        loop {
            if self.failure.load(Ordering::SeqCst) {
                return false;
            }
            let successes = self.successes.load(Ordering::SeqCst);
            if successes >= self.total {
                debug_assert_eq!(
                    successes, self.total,
                    "should match: ensure `new` is called with the correct value for `total`"
                );
                return true;
            }
            self.notify.notified().await;
            self.notify.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;
    use futures::future;
    use tokio::task;

    #[derive(Debug)]
    struct TaskOutcome {
        elapsed: Duration,
        success: bool,
    }

    async fn connect(
        tasks: ConnectionTasks,
        connection_delay_ms: u64,
        connect_should_succeed: bool,
    ) -> TaskOutcome {
        let start = Instant::now();
        // A delay to emulate connecting to the node.
        tokio::time::sleep(Duration::from_millis(connection_delay_ms)).await;

        if connect_should_succeed {
            tasks.register_success();
        } else {
            tasks.register_failure();
        };

        let success = tasks.wait().await;
        TaskOutcome {
            elapsed: start.elapsed(),
            success,
        }
    }

    #[tokio::test]
    async fn should_wait_for_all_to_succeed() {
        // Spawn 3 connection attempts, have all succeed.  Should cause all tasks to return `true`
        // after the maximum delay from all tasks.
        const MAX_DELAY_MS: u64 = 250;
        let tasks = ConnectionTasks::new(3);
        let join_handles = vec![
            task::spawn(connect(tasks.clone(), MAX_DELAY_MS / 20, true)),
            task::spawn(connect(tasks.clone(), MAX_DELAY_MS / 10, true)),
            task::spawn(connect(tasks.clone(), MAX_DELAY_MS, true)),
        ];

        let results = future::join_all(join_handles).await;
        for result in results {
            let outcome = result.unwrap();
            assert!(outcome.elapsed >= Duration::from_millis(MAX_DELAY_MS));
            assert!(outcome.success)
        }
    }

    #[tokio::test]
    async fn should_fail_if_any_fails() {
        // Spawn 3 connection attempts, have the second one fail.  Should cause all tasks to return
        // `false` with the first and second returning before the longer delay of the third task.
        const TASK_COUNT: usize = 3;
        const FAIL_DELAY_MS: u64 = 250;
        const MAX_DELAY_MS: u64 = FAIL_DELAY_MS + 100;
        let tasks = ConnectionTasks::new(TASK_COUNT);
        let join_handles = vec![
            task::spawn(connect(tasks.clone(), FAIL_DELAY_MS / 20, true)),
            task::spawn(connect(tasks.clone(), FAIL_DELAY_MS, false)),
            task::spawn(connect(tasks.clone(), MAX_DELAY_MS, true)),
        ];

        let results = future::join_all(join_handles).await;

        for (index, result) in results.into_iter().enumerate() {
            let outcome = result.unwrap();
            if index < TASK_COUNT - 1 {
                assert!(outcome.elapsed >= Duration::from_millis(FAIL_DELAY_MS));
                assert!(outcome.elapsed < Duration::from_millis(MAX_DELAY_MS));
            } else {
                assert!(outcome.elapsed >= Duration::from_millis(MAX_DELAY_MS));
            }
            assert!(!outcome.success)
        }
    }
}
