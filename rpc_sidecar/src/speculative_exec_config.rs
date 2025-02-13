use std::net::{IpAddr, Ipv4Addr};

use datasize::DataSize;
use serde::Deserialize;

/// Default binding address for the speculative execution RPC HTTP server.
///
/// Uses a fixed port per node, but binds on any interface.
const DEFAULT_IP_ADDRESS: IpAddr = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));
const DEFAULT_PORT: u16 = 1;
/// Default rate limit in qps.
const DEFAULT_QPS_LIMIT: u32 = 1;
/// Default max body bytes (2.5MB).
const DEFAULT_MAX_BODY_BYTES: u64 = 2_621_440;
/// Default CORS origin.
const DEFAULT_CORS_ORIGIN: &str = "";

/// JSON-RPC HTTP server configuration.
#[derive(Clone, DataSize, Debug, Deserialize, PartialEq, Eq)]
// Disallow unknown fields to ensure config files and command-line overrides contain valid keys.
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Setting to enable the HTTP server.
    pub enable_server: bool,
    /// IP address to bind JSON-RPC speculative execution server to.
    pub ip_address: IpAddr,
    /// Port to bind JSON-RPC speculative execution server to.
    pub port: u16,
    /// Maximum rate limit in queries per second.
    pub qps_limit: u32,
    /// Maximum number of bytes to accept in a single request body.
    pub max_body_bytes: u64,
    /// CORS origin.
    pub cors_origin: String,
}

impl Config {
    /// Creates a default instance for `RpcServer`.
    #[must_use]
    pub fn new() -> Self {
        Config {
            enable_server: false,
            ip_address: DEFAULT_IP_ADDRESS,
            port: DEFAULT_PORT,
            qps_limit: DEFAULT_QPS_LIMIT,
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            cors_origin: DEFAULT_CORS_ORIGIN.to_string(),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Config::new()
    }
}
