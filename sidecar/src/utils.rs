#[cfg(test)]
use crate::testing::mock_node::tests::MockNode;
#[cfg(test)]
use anyhow::Error;
#[cfg(test)]
use std::time::Duration;
use std::{
    fmt::{self, Debug, Display, Formatter},
    io,
    net::{SocketAddr, ToSocketAddrs},
};
#[cfg(test)]
use tokio::sync::mpsc::Receiver;
#[cfg(test)]
use tokio::time::timeout;

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
