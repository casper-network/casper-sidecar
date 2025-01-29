use std::net::{IpAddr, Ipv4Addr};

use datasize::DataSize;
use serde::Deserialize;
use thiserror::Error;

use crate::SpeculativeExecConfig;

/// Default binding address for the JSON-RPC HTTP server.
///
/// Uses a fixed port per node, but binds on any interface.
const DEFAULT_IP_ADDRESS: IpAddr = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));
const DEFAULT_PORT: u16 = 0;
/// Default rate limit in qps.
const DEFAULT_QPS_LIMIT: u64 = 100;
/// Default max body bytes.  This is 2.5MB which should be able to accommodate the largest valid
/// JSON-RPC request, which would be an "account_put_deploy".
const DEFAULT_MAX_BODY_BYTES: u32 = 2_621_440;
/// Default CORS origin.
const DEFAULT_CORS_ORIGIN: &str = "";

#[derive(Error, Debug)]
pub enum FieldParseError {
    #[error("failed to parse field {} with error: {}", .field_name, .error)]
    ParseError {
        field_name: &'static str,
        error: String,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
// Disallow unknown fields to ensure config files and command-line overrides contain valid keys.
#[serde(deny_unknown_fields)]
#[cfg_attr(any(feature = "testing", test), derive(Default))]
pub struct RpcServerConfig {
    pub main_server: RpcConfig,
    pub speculative_exec_server: Option<SpeculativeExecConfig>,
    pub node_client: NodeClientConfig,
}

/// JSON-RPC HTTP server configuration.
#[derive(Clone, DataSize, Debug, Deserialize, PartialEq, Eq)]
// Disallow unknown fields to ensure config files and command-line overrides contain valid keys.
#[serde(deny_unknown_fields)]
pub struct RpcConfig {
    /// Setting to enable the HTTP server.
    pub enable_server: bool,
    /// IP address to bind JSON-RPC HTTP server to.
    pub ip_address: IpAddr,
    /// TCP port to bind JSON-RPC HTTP server to.
    pub port: u16,
    /// Maximum rate limit in queries per second.
    pub qps_limit: u64,
    /// Maximum number of bytes to accept in a single request body.
    pub max_body_bytes: u32,
    /// CORS origin.
    pub cors_origin: String,
}

impl RpcConfig {
    /// Creates a default instance for `RpcServer`.
    pub fn new() -> Self {
        RpcConfig {
            enable_server: true,
            ip_address: DEFAULT_IP_ADDRESS,
            port: DEFAULT_PORT,
            qps_limit: DEFAULT_QPS_LIMIT,
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            cors_origin: DEFAULT_CORS_ORIGIN.to_string(),
        }
    }
}

impl Default for RpcConfig {
    fn default() -> Self {
        RpcConfig::new()
    }
}

/// Default address to connect to the node.
// Change this to SocketAddr, once SocketAddr::new is const stable.
const DEFAULT_NODE_CONNECT_IP_ADDRESS: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
const DEFAULT_NODE_CONNECT_PORT: u16 = 28104;
/// Default maximum payload size.
const DEFAULT_MAX_PAYLOAD_SIZE: u32 = 4 * 1024 * 1024;
/// Default message timeout in seconds.
const DEFAULT_MESSAGE_TIMEOUT_SECS: u64 = 30;
/// Default timeout for client access.
const DEFAULT_CLIENT_ACCESS_TIMEOUT_SECS: u64 = 10;
/// Default exponential backoff base delay.
const DEFAULT_EXPONENTIAL_BACKOFF_BASE_MS: u64 = 1000;
/// Default exponential backoff maximum delay.
const DEFAULT_EXPONENTIAL_BACKOFF_MAX_MS: u64 = 64_000;
/// Default exponential backoff coefficient.
const DEFAULT_EXPONENTIAL_BACKOFF_COEFFICIENT: u64 = 2;
/// Default keep alive timeout milliseconds.
const DEFAULT_KEEPALIVE_TIMEOUT_MS: u64 = 1_000;
/// Default max attempts
const DEFAULT_EXPONENTIAL_BACKOFF_MAX_ATTEMPTS: u32 = 3;

/// Node client configuration.
#[derive(Clone, DataSize, Debug, Deserialize, PartialEq, Eq)]
// Disallow unknown fields to ensure config files and command-line overrides contain valid keys.
#[serde(deny_unknown_fields)]
pub struct NodeClientConfig {
    /// IP address of the node.
    pub ip_address: IpAddr,
    /// Port of the node.
    pub port: u16,
    /// Maximum size of a message in bytes.
    pub max_message_size_bytes: u32,
    /// Message transfer timeout in seconds.
    pub message_timeout_secs: u64,
    /// Timeout specifying how long to wait for binary port client to be available.
    // Access to the client is synchronized.
    pub client_access_timeout_secs: u64,
    /// The amount of ms to wait between sending keepalive requests.
    pub keepalive_timeout_ms: u64,
    /// Configuration for exponential backoff to be used for re-connects.
    pub exponential_backoff: ExponentialBackoffConfig,
}

impl NodeClientConfig {
    /// Creates a default instance for `NodeClientConfig`.
    pub fn new() -> Self {
        NodeClientConfig {
            ip_address: DEFAULT_NODE_CONNECT_IP_ADDRESS,
            port: DEFAULT_NODE_CONNECT_PORT,
            max_message_size_bytes: DEFAULT_MAX_PAYLOAD_SIZE,
            message_timeout_secs: DEFAULT_MESSAGE_TIMEOUT_SECS,
            client_access_timeout_secs: DEFAULT_CLIENT_ACCESS_TIMEOUT_SECS,
            keepalive_timeout_ms: DEFAULT_KEEPALIVE_TIMEOUT_MS,
            exponential_backoff: ExponentialBackoffConfig {
                initial_delay_ms: DEFAULT_EXPONENTIAL_BACKOFF_BASE_MS,
                max_delay_ms: DEFAULT_EXPONENTIAL_BACKOFF_MAX_MS,
                coefficient: DEFAULT_EXPONENTIAL_BACKOFF_COEFFICIENT,
                max_attempts: DEFAULT_EXPONENTIAL_BACKOFF_MAX_ATTEMPTS,
            },
        }
    }

    /// Creates an instance of `NodeClientConfig` with specified listening port.
    #[cfg(any(feature = "testing", test))]
    pub fn new_with_port(port: u16) -> Self {
        let localhost = IpAddr::V4(Ipv4Addr::LOCALHOST);
        NodeClientConfig {
            ip_address: localhost,
            port,
            max_message_size_bytes: DEFAULT_MAX_PAYLOAD_SIZE,
            message_timeout_secs: DEFAULT_MESSAGE_TIMEOUT_SECS,
            client_access_timeout_secs: DEFAULT_CLIENT_ACCESS_TIMEOUT_SECS,
            keepalive_timeout_ms: DEFAULT_KEEPALIVE_TIMEOUT_MS,
            exponential_backoff: ExponentialBackoffConfig {
                initial_delay_ms: DEFAULT_EXPONENTIAL_BACKOFF_BASE_MS,
                max_delay_ms: DEFAULT_EXPONENTIAL_BACKOFF_MAX_MS,
                coefficient: DEFAULT_EXPONENTIAL_BACKOFF_COEFFICIENT,
                max_attempts: DEFAULT_EXPONENTIAL_BACKOFF_MAX_ATTEMPTS,
            },
        }
    }

    /// Creates an instance of `NodeClientConfig` with specified listening port and maximum number
    /// of reconnection retries.
    #[cfg(any(feature = "testing", test))]
    pub fn new_with_port_and_retries(port: u16, num_of_retries: u32) -> Self {
        let localhost = IpAddr::V4(Ipv4Addr::LOCALHOST);
        NodeClientConfig {
            ip_address: localhost,
            port,
            max_message_size_bytes: DEFAULT_MAX_PAYLOAD_SIZE,
            message_timeout_secs: DEFAULT_MESSAGE_TIMEOUT_SECS,
            client_access_timeout_secs: DEFAULT_CLIENT_ACCESS_TIMEOUT_SECS,
            keepalive_timeout_ms: DEFAULT_KEEPALIVE_TIMEOUT_MS,
            exponential_backoff: ExponentialBackoffConfig {
                initial_delay_ms: 500,
                max_delay_ms: 3000,
                coefficient: 3,
                max_attempts: num_of_retries,
            },
        }
    }
}

impl Default for NodeClientConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Exponential backoff configuration for re-connects.
#[derive(Clone, DataSize, Debug, Deserialize, PartialEq, Eq)]
// Disallow unknown fields to ensure config files and command-line overrides contain valid keys.
#[serde(deny_unknown_fields)]
pub struct ExponentialBackoffConfig {
    /// Initial wait time before the first re-connect attempt.
    pub initial_delay_ms: u64,
    /// Maximum wait time between re-connect attempts.
    pub max_delay_ms: u64,
    /// The multiplier to apply to the previous delay to get the next delay.
    pub coefficient: u64,
    /// Maximum number of connection attempts.
    pub max_attempts: u32,
}
