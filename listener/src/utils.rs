use std::{
    fmt::{self, Debug, Display, Formatter},
    io,
    net::{SocketAddr, ToSocketAddrs},
};

use thiserror::Error;

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
