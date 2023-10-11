use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    Mutex,
};
use tokio_util::sync::CancellationToken;

/// An object that allows killing connections if a period of inactivity has been detected.
/// The node exposes keep-alive connections and issues a periodic dummy (comment) message that says just `:`.
/// This way even if there are no messages for an extended period of time we can tell that the connection to node is alive and well.
/// But for some reason the eventsource implementation doesn't support timing out when a period of inactivity happens.
/// Reqwest theoretically has connection properties that should allow to timeout after a period of receiving no data, but they don't seem to actually timeout if there are no keepalive messages.
/// The KeepAliveMonitor has the responsibility to count ticks. If more time than configured happened since last "tick", we send a posion pill via cancelling a CancellationToken.
pub struct KeepAliveMonitor {
    /// Address of the endpoint which the KeepAliveMonitor needs to observe
    cancellation_token: CancellationToken,
    /// Time the check job sleeps between checks
    sleep_between_checks: Duration,
    /// If KeepAliveMonitor will not see any new data from the endpoint in at least this duration, it will cancel the `cancellation_token`
    no_message_timeout: Duration,
    /// Internal state of KeepAliveMonitor, it is the instant of time when the last bytes were observed from `bind_address`
    /// This mutex could be turned into an RwLock, but for now there is only one reader and one writer task
    last_message_seen_at: Arc<Mutex<Option<Instant>>>,
    /// This receiver should get messages every time there is something happening on the datasource that we are monitoring for inactivity.
    receiver: Arc<Mutex<Receiver<()>>>,
    /// Internal queue which collects "ticks". A "tick" happens every time we observe any message from `receiver` field.
    sender: Sender<()>,
}

impl KeepAliveMonitor {
    /// The `tick` should be called every time we observe data on the monitored datasource.
    /// Internally, not every `tick` might actually update the `last_message_seen_at`.
    /// We wait for a certain amount of time for the send to happen, if it fails it means that there is already a full queue of ticks waiting to be processed.
    /// And we don't need to have the exact value of `last_message_seen_at`, we just need to keep track if the data source got stale.
    pub async fn tick(&self) {
        let _ = self
            .sender
            .send_timeout((), Duration::from_millis(20))
            .await;
    }

    /// Assembles a new `KeepAliveMonitor`. It still doesn't collect data, you need to call the `start` function for that.
    pub fn new(sleep_between_checks: Duration, no_message_timeout: Duration) -> Self {
        let cancellation_token = CancellationToken::new();
        let last_message_seen_at = Arc::new(Mutex::new(None));
        let (tx, rx) = channel(10);
        KeepAliveMonitor {
            cancellation_token,
            sleep_between_checks,
            no_message_timeout,
            last_message_seen_at,
            receiver: Arc::new(Mutex::new(rx)),
            sender: tx,
        }
    }

    pub fn get_cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    /// Spawns tasks responsible for asynchronous checks of the monitored datasource.
    /// If a configured time of inactivity is detected, `cancellation_token` is cancelled.
    pub async fn start(&self) {
        self.spawn_last_seen_checker_task(
            self.last_message_seen_at.clone(),
            self.sleep_between_checks,
            self.no_message_timeout,
            self.cancellation_token.clone(),
        );
        self.spawn_data_observing_task();
    }

    fn spawn_data_observing_task(&self) {
        let receiver = self.receiver.clone();
        let last_message_seen_at = self.last_message_seen_at.clone();
        tokio::spawn(async move {
            let mut guard = receiver.lock().await;
            while (guard.recv().await).is_some() {
                let now = Some(Instant::now());
                let mut guard = last_message_seen_at.lock().await;
                *guard = now;
                drop(guard);
            }
        });
    }

    fn spawn_last_seen_checker_task(
        &self,
        last_activity_holder: Arc<Mutex<Option<Instant>>>,
        sleep_between_checks: Duration,
        no_message_timeout: Duration,
        cancellation_token: CancellationToken,
    ) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(sleep_between_checks).await;
                let mut guard = last_activity_holder.lock().await;
                match &mut *guard {
                    Some(last_seen_at) => {
                        if last_seen_at.elapsed() > no_message_timeout {
                            cancellation_token.cancel();
                            break;
                        }
                    }
                    None => {
                        // This scenario shouldn't happen often. It means that there was no data observed before the first check happened.
                        // We are setting last_seen_at value to now so that the keep alive can fail in the off chance that the process
                        // which is producing data hung before first message was produced
                        *guard = Some(Instant::now());
                    }
                }
                drop(guard);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use crate::keep_alive_monitor::KeepAliveMonitor;
    use std::{sync::Arc, time::Duration};
    use tokio::{select, time::sleep};

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn should_cancel_when_first_check_finds_no_activity() {
        let monitor = KeepAliveMonitor::new(Duration::from_secs(1), Duration::from_secs(2));
        monitor.start().await;
        let cancellation_token = monitor.get_cancellation_token();
        select! {
            _ = cancellation_token.cancelled() => {},
            _ = sleep(Duration::from_secs(10)) => {
                unreachable!()
            },
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn should_not_cancel_if_endpoint_produces_data() {
        let monitor = Arc::new(KeepAliveMonitor::new(
            Duration::from_secs(10),
            Duration::from_secs(30),
        ));
        sse_server(monitor.clone(), 1);
        monitor.start().await;
        let cancellation_token = monitor.get_cancellation_token();
        select! {
            _ = cancellation_token.cancelled() => {
                unreachable!()
            },
            _ = sleep(Duration::from_secs(15)) => {
            },
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn should_cancel_if_no_activity_for_prolonged_period_of_time() {
        let monitor = Arc::new(KeepAliveMonitor::new(
            Duration::from_secs(1),
            Duration::from_secs(3),
        ));
        sse_server(monitor.clone(), 1500); //1500 seconds of interval to make sure that the monitor won't see activity
        monitor.start().await;
        let cancellation_token = monitor.get_cancellation_token();
        select! {
            _ = cancellation_token.cancelled() => {
            },
            _ = sleep(Duration::from_secs(15)) => {
                unreachable!()
            },
        }
    }

    fn sse_server(monitor: Arc<KeepAliveMonitor>, interval_in_seconds: u64) {
        tokio::spawn(async move {
            monitor.tick().await;
            sleep(Duration::from_secs(interval_in_seconds)).await;
        });
    }
}
