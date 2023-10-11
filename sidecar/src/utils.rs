#[cfg(test)]
use crate::integration_tests::{build_test_config, start_sidecar};
#[cfg(feature = "additional-metrics")]
use crate::metrics::EVENTS_PROCESSED_PER_SECOND;
#[cfg(test)]
use crate::testing::mock_node::tests::MockNode;
#[cfg(test)]
use crate::testing::testing_config::TestingConfig;
#[cfg(test)]
use anyhow::Error;
#[cfg(feature = "additional-metrics")]
use std::sync::Arc;
#[cfg(any(feature = "additional-metrics", test))]
use std::time::Duration;
#[cfg(feature = "additional-metrics")]
use std::time::Instant;
use std::{
    fmt::{self, Debug, Display, Formatter},
    io,
    net::{SocketAddr, ToSocketAddrs},
};
#[cfg(test)]
use tempfile::TempDir;
#[cfg(test)]
use tokio::sync::mpsc::Receiver;
#[cfg(feature = "additional-metrics")]
use tokio::sync::{
    mpsc::{channel, Sender},
    Mutex,
};
#[cfg(test)]
use tokio::time::timeout;
#[cfg(feature = "additional-metrics")]
use tracing::error;

use thiserror::Error;
use warp::{reject, Filter};

#[derive(Debug)]
pub struct Unexpected(pub(super) anyhow::Error);
impl reject::Reject for Unexpected {}

#[derive(Debug)]
pub struct InvalidPath;
impl reject::Reject for InvalidPath {}

/// DNS resolution error.
#[derive(Debug, Error)]
#[error("could not resolve `{address}`: {kind}")]
pub struct ResolveAddressError {
    /// Address that failed to resolve.
    address: String,
    /// Reason for resolution failure.
    kind: ResolveAddressErrorKind,
}

/// DNS resolution error kind.
#[derive(Debug)]
enum ResolveAddressErrorKind {
    /// Resolve returned an error.
    ErrorResolving(io::Error),
    /// Resolution did not yield any address.
    NoAddressFound,
}

impl Display for ResolveAddressErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ResolveAddressErrorKind::ErrorResolving(err) => {
                write!(f, "could not run dns resolution: {}", err)
            }
            ResolveAddressErrorKind::NoAddressFound => {
                write!(f, "no addresses found")
            }
        }
    }
}

/// Parses a network address from a string, with DNS resolution.
pub(crate) fn resolve_address(address: &str) -> Result<SocketAddr, ResolveAddressError> {
    address
        .to_socket_addrs()
        .map_err(|err| ResolveAddressError {
            address: address.to_string(),
            kind: ResolveAddressErrorKind::ErrorResolving(err),
        })?
        .next()
        .ok_or_else(|| ResolveAddressError {
            address: address.to_string(),
            kind: ResolveAddressErrorKind::NoAddressFound,
        })
}

/// An error starting one of the HTTP servers.
#[derive(Debug, Error)]
pub(crate) enum ListeningError {
    /// Failed to resolve address.
    #[error("failed to resolve network address: {0}")]
    ResolveAddress(ResolveAddressError),

    /// Failed to listen.
    #[error("failed to listen on {address}: {error}")]
    Listen {
        /// The address attempted to listen on.
        address: SocketAddr,
        /// The failure reason.
        error: Box<dyn std::error::Error + Send + Sync>,
    },
}

#[cfg(test)]
pub(crate) fn display_duration(duration: Duration) -> String {
    // less than a second
    if duration.as_millis() < 1000 {
        format!("{}ms", duration.as_millis())
        // more than a minute
    } else if duration.as_secs() > 60 {
        let (minutes, seconds) = (duration.as_secs() / 60, duration.as_secs() % 60);
        format!("{}min. {}s", minutes, seconds)
        // over a second / under a minute
    } else {
        format!("{}s", duration.as_secs())
    }
}

#[cfg(test)]
/// Forces a stop on the given nodes and waits until all starts finish. Will timeout if the nodes can't start in 3 minutes.
pub(crate) async fn start_nodes_and_wait(nodes: Vec<&mut MockNode>) -> Vec<()> {
    let mut futures = vec![];
    for node in nodes {
        futures.push(node.start());
    }
    timeout(Duration::from_secs(180), futures::future::join_all(futures))
        .await
        .unwrap()
}

#[cfg(test)]
/// wait_for_n_messages waits at most the `timeout_after` for observing `n` messages received on the `receiver`
/// If the receiver returns a None the function will finish early
pub(crate) async fn wait_for_n_messages<T: Send + Sync + 'static>(
    n: usize,
    mut receiver: Receiver<T>,
    timeout_after: Duration,
) -> Receiver<T> {
    let join_handle = tokio::spawn(async move {
        for _ in 0..n {
            if receiver.recv().await.is_none() {
                break;
            }
        }
        receiver
    });
    match timeout(timeout_after, join_handle).await {
        Ok(res) => res.map_err(|err| Error::msg(format!("Failed to wait for receiver, {}", err))),
        Err(_) => Err(Error::msg("Waiting for messages timed out")),
    }
    .unwrap()
}

#[cfg(test)]
/// Forces a stop on the given nodes and waits until the stop happens. Will timeout if the nodes can't stop in 3 minutes.
pub(crate) async fn stop_nodes_and_wait(nodes: Vec<&mut MockNode>) -> Vec<()> {
    let mut futures = vec![];
    for node in nodes {
        futures.push(node.stop());
    }
    timeout(Duration::from_secs(180), futures::future::join_all(futures))
        .await
        .unwrap()
}

#[cfg(test)]
pub(crate) fn any_string_contains(data: &[String], infix: String) -> bool {
    let infix_str = infix.as_str();
    data.iter().any(|x| x.contains(infix_str))
}

/// Handle the case where no filter URL was specified after the root address (HOST:PORT).
/// Return: a message that an invalid path was provided.
/// Example: curl http://127.0.0.1:18888
/// {"code":400,"message":"Invalid request path provided"}
pub fn root_filter() -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone
{
    warp::path::end()
        .and_then(|| async { Err::<String, warp::Rejection>(warp::reject::custom(InvalidPath)) })
}

#[cfg(test)]
pub struct MockNodeTestProperties {
    pub testing_config: TestingConfig,
    pub temp_storage_dir: TempDir,
    pub node_port_for_sse_connection: u16,
    pub node_port_for_rest_connection: u16,
    pub event_stream_server_port: u16,
}

#[cfg(test)]
pub async fn prepare_one_node_and_start(node_mock: &mut MockNode) -> MockNodeTestProperties {
    let (
        testing_config,
        temp_storage_dir,
        node_port_for_sse_connection,
        node_port_for_rest_connection,
        event_stream_server_port,
    ) = build_test_config();
    node_mock.set_sse_port(node_port_for_sse_connection);
    node_mock.set_rest_port(node_port_for_rest_connection);
    start_nodes_and_wait(vec![node_mock]).await;
    start_sidecar(testing_config.inner()).await;
    MockNodeTestProperties {
        testing_config,
        temp_storage_dir,
        node_port_for_sse_connection,
        node_port_for_rest_connection,
        event_stream_server_port,
    }
}

#[cfg(feature = "additional-metrics")]
struct MetricsData {
    last_measurement: Instant,
    observed_events: u64,
}

#[cfg(feature = "additional-metrics")]
pub fn start_metrics_thread(module_name: String) -> Sender<()> {
    let (metrics_queue_tx, mut metrics_queue_rx) = channel(10000);
    let metrics_data = Arc::new(Mutex::new(MetricsData {
        last_measurement: Instant::now(),
        observed_events: 0,
    }));
    let metrics_data_for_thread = metrics_data.clone();
    tokio::spawn(async move {
        let metrics_data = metrics_data_for_thread;
        let sleep_for = Duration::from_secs(30);
        loop {
            tokio::time::sleep(sleep_for).await;
            let mut guard = metrics_data.lock().await;
            let duration = guard.last_measurement.elapsed();
            let number_of_observed_events = guard.observed_events;
            guard.observed_events = 0;
            guard.last_measurement = Instant::now();
            drop(guard);
            let seconds = duration.as_secs_f64();
            let events_per_second = (number_of_observed_events as f64) / seconds;
            EVENTS_PROCESSED_PER_SECOND
                .with_label_values(&[module_name.as_str()])
                .set(events_per_second);
        }
    });
    let metrics_data_for_thread = metrics_data.clone();
    tokio::spawn(async move {
        let metrics_data = metrics_data_for_thread;
        while let Some(_) = metrics_queue_rx.recv().await {
            let mut guard = metrics_data.lock().await;
            guard.observed_events += 1;
            drop(guard);
        }
    });
    metrics_queue_tx
}
