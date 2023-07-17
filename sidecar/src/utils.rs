#[cfg(test)]
use std::time::Duration;
use std::{
    fmt::{self, Debug, Display, Formatter},
    io,
    net::{SocketAddr, ToSocketAddrs},
};

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

/// Handle the case where no filter URL was specified after the root address (HOST:PORT).
/// Return: a message that an invalid path was provided.
/// Example: curl http://127.0.0.1:18888
/// {"code":400,"message":"Invalid request path provided"}
pub fn root_filter() -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone
{
    warp::path::end()
        .and_then(|| async { Err::<String, warp::Rejection>(warp::reject::custom(InvalidPath)) })
}
