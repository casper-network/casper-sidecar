use casper_event_types::metrics;
use futures::StreamExt;
use reqwest::Client;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{error, trace};
use url::Url;

/// An object that allows killing connections if a period of inactivity has been detected.
/// The node exposes keep-alive connections and issues a periodic dummy (comment) message that says just `:`.
/// This way even if there are no messages for an extended period of time we can tell that the connection to node is alive and well.
/// But for some reason the eventsource implementation doesn't support timing out when a period of inactivity happens.
/// Reqwest theoretically has connection properties that should allow to timeout after a period of receiving no data, but they don't seem to actually timeout if there are no keepalive messages.
/// The KeepAliveMonitor has the responsibility to track bytes coming in from a nodes endpoint, track when the last message happened and send a poison pill message if the time since the last received exceeded a configurable threshold.
pub struct KeepAliveMonitor {
    /// Address of the endpoint which the KeepAliveMonitor needs to observe
    bind_address: Url,
    /// Address of the endpoint which the KeepAliveMonitor needs to observe
    cancellation_token: Arc<CancellationToken>,
    /// KeepAliveMonitor will try connecting to bind_address at most this amount of time
    connection_timeout: Duration,
    /// Time the check job sleeps between checks
    sleep_between_checks: Duration,
    /// If KeepAliveMonitor will not see any new data from the endpoint in at least this duration, it will cancel the `cancellation_token`
    no_message_timeout: Duration,
    /// Internal state of KeepAliveMonitor, it is the instant of time when the last bytes were observed from `bind_address`
    last_message_seen_at: Arc<Mutex<Option<Instant>>>,
}

impl KeepAliveMonitor {
    pub fn new(
        bind_address: Url,
        connection_timeout: Duration,
        sleep_between_checks: Duration,
        no_message_timeout: Duration,
    ) -> Self {
        let cancellation_token = Arc::new(CancellationToken::new());
        let last_message_seen_at = Arc::new(Mutex::new(None));
        KeepAliveMonitor {
            bind_address,
            cancellation_token,
            connection_timeout,
            sleep_between_checks,
            no_message_timeout,
            last_message_seen_at,
        }
    }

    pub fn get_cancellation_token(&self) -> Arc<CancellationToken> {
        self.cancellation_token.clone()
    }

    pub async fn start(&self) {
        self.spawn_last_seen_updater_thread(
            self.last_message_seen_at.clone(),
            self.bind_address.clone(),
            self.connection_timeout,
            self.cancellation_token.clone(),
        );
        self.spawn_last_seen_checker_thread(
            self.last_message_seen_at.clone(),
            self.sleep_between_checks,
            self.no_message_timeout,
            self.bind_address.clone(),
            self.cancellation_token.clone(),
        );
    }

    fn spawn_last_seen_checker_thread(
        &self,
        last_activity_holder: Arc<Mutex<Option<Instant>>>,
        sleep_between_checks: Duration,
        no_message_timeout: Duration,
        bind_address: Url,
        cancellation_token: Arc<CancellationToken>,
    ) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(sleep_between_checks).await;
                log_event(bind_address.clone().as_str(), "last_seen_checker_tick");
                let mut guard = last_activity_holder.lock().await;
                match &mut *guard {
                    Some(last_seen_at) => {
                        if last_seen_at.elapsed() > no_message_timeout {
                            count_internal_event(
                                bind_address.clone().as_str(),
                                "no_activity_timeout",
                            );
                            log_event(bind_address.clone().as_str(), "no_activity_timeout");
                            cancellation_token.cancel();
                            break;
                        } else {
                            log_event(bind_address.clone().as_str(), "last_seen_checker_ok");
                        }
                    }
                    None => {
                        // This is an erroneous situation. By the time first check happens there already should be some data read from the endpoint
                        log_event(bind_address.clone().as_str(), "last_seen_checker_no_value");
                        cancellation_token.cancel();
                    }
                }
                drop(guard);
            }
        });
    }

    fn spawn_last_seen_updater_thread(
        &self,
        arc_for_updater: Arc<Mutex<Option<Instant>>>,
        bind_address_for_updater: Url,
        connection_timeout: Duration,
        cancellation_token: Arc<CancellationToken>,
    ) {
        tokio::spawn(async move {
            let arc = arc_for_updater;
            let bind_address = bind_address_for_updater;
            let client_builder = Client::builder().connect_timeout(connection_timeout);
            let client_res = client_builder.build();
            if let Err(err) = client_res {
                let error_message = format!(
                    "Couldn't create client in KeepAliveMonitor for address {}. Error {}",
                    bind_address.clone(),
                    err
                );
                error!(error_message);
                //If we can't connect - force the connection to restart
                cancellation_token.cancel();
                return;
            }
            let response_res = client_res.unwrap().get(bind_address.clone()).send().await;
            if let Err(err) = response_res {
                let error_message = format!(
                    "Couldn't get response in KeepAliveMonitor for address {}. Error {}",
                    bind_address.clone(),
                    err
                );
                error!(error_message);
                //If we can't connect - force the connection to restart
                cancellation_token.cancel();
                return;
            }
            let mut stream = response_res.unwrap().bytes_stream();
            while let Some(event) = stream.next().await {
                match event {
                    Ok(_) => {
                        log_event(bind_address.as_str(), "bytes_observed");
                        let mut guard = arc.lock().await;
                        *guard = Some(Instant::now());
                        drop(guard);
                    }
                    Err(err) => {
                        log_event(bind_address.as_str(), "bytes_observed_error");
                        error!("Error: {}", err);
                        cancellation_token.cancel();
                        break;
                    }
                }
            }
        });
    }
}

#[inline]
fn log_event(bind_address: &str, event: &str) {
    let message = format!(
        "[KeepAliveMonitor] Bind address: {}. Event {}.",
        bind_address, event
    );
    trace!(message);
}

fn count_internal_event(category: &str, reason: &str) {
    metrics::INTERNAL_EVENTS
        .with_label_values(&[category, reason])
        .inc();
}

#[cfg(test)]
mod tests {
    use crate::keep_alive_monitor::KeepAliveMonitor;
    use futures_util::StreamExt;
    use portpicker::pick_unused_port;
    use std::convert::Infallible;
    use std::time::Duration;
    use tokio::{
        select,
        time::{interval, sleep},
    };
    use tokio_stream::wrappers::IntervalStream;
    use url::Url;
    use warp::{sse::Event, Filter};

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn should_cancel_when_first_check_finds_no_activity() {
        let url = Url::parse("http://localhost:9999").unwrap();
        let monitor = KeepAliveMonitor::new(
            url,
            Duration::from_secs(100),
            Duration::from_secs(1),
            Duration::from_secs(2),
        );
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
        let port = pick_unused_port().unwrap();
        sse_server(port, 1);
        let url = Url::parse(format!("http://localhost:{}/ticks", port).as_str()).unwrap();
        let monitor = KeepAliveMonitor::new(
            url,
            Duration::from_secs(100),
            Duration::from_secs(1),
            Duration::from_secs(3),
        );
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
        let port = pick_unused_port().unwrap();
        sse_server(port, 1500); //1500 seconds of interval to make sure that the monitor won't see activity
        let url = Url::parse(format!("http://localhost:{}/ticks", port).as_str()).unwrap();
        let monitor = KeepAliveMonitor::new(
            url,
            Duration::from_secs(100),
            Duration::from_secs(1),
            Duration::from_secs(3),
        );
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

    fn sse_server(port: u16, interval_in_seconds: u64) {
        let routes = warp::path("ticks").and(warp::get()).map(move || {
            let mut counter: u64 = 0;
            // create server event source
            let interval = interval(Duration::from_secs(interval_in_seconds));
            let stream = IntervalStream::new(interval);
            let event_stream = stream.map(move |_| {
                counter += 1;
                Ok::<Event, Infallible>(warp::sse::Event::default().data(counter.to_string()))
            });
            // reply using server-sent events
            warp::sse::reply(event_stream)
        });
        tokio::spawn(async move {
            warp::serve(routes).run(([127, 0, 0, 1], port)).await;
        });
    }
}
