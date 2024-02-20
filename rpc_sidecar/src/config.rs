use std::{
    convert::{TryFrom, TryInto},
    net::{IpAddr, Ipv4Addr, SocketAddr},
};

use datasize::DataSize;
use serde::Deserialize;
use thiserror::Error;

use crate::SpeculativeExecConfig;

/// Default binding address for the JSON-RPC HTTP server.
///
/// Uses a fixed port per node, but binds on any interface.
const DEFAULT_ADDRESS: &str = "0.0.0.0:0";
/// Default rate limit in qps.
const DEFAULT_QPS_LIMIT: u64 = 100;
/// Default max body bytes.  This is 2.5MB which should be able to accommodate the largest valid
/// JSON-RPC request, which would be an "account_put_deploy".
const DEFAULT_MAX_BODY_BYTES: u32 = 2_621_440;
/// Default CORS origin.
const DEFAULT_CORS_ORIGIN: &str = "";

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
// Disallow unknown fields to ensure config files and command-line overrides contain valid keys.
#[serde(deny_unknown_fields)]
pub struct RpcServerConfigTarget {
    pub main_server: RpcConfig,
    pub speculative_exec_server: Option<SpeculativeExecConfig>,
    pub node_client: NodeClientConfigTarget,
}

impl TryFrom<RpcServerConfigTarget> for RpcServerConfig {
    type Error = FieldParseError;
    fn try_from(value: RpcServerConfigTarget) -> Result<Self, Self::Error> {
        let node_client = value.node_client.try_into().map_err(|e: FieldParseError| {
            FieldParseError::ParseError {
                field_name: "node_client",
                error: e.to_string(),
            }
        })?;
        Ok(RpcServerConfig {
            main_server: value.main_server,
            speculative_exec_server: value.speculative_exec_server,
            node_client,
        })
    }
}

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
    /// Address to bind JSON-RPC HTTP server to.
    pub address: String,
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
            address: DEFAULT_ADDRESS.to_string(),
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
const DEFAULT_NODE_CONNECT_ADDRESS: (IpAddr, u16) = (IpAddr::V4(Ipv4Addr::LOCALHOST), 28104);
/// Default maximum payload size.
const DEFAULT_MAX_NODE_PAYLOAD_SIZE: u32 = 4 * 1024 * 1024;
/// Default request limit.
const DEFAULT_NODE_REQUEST_LIMIT: u16 = 3;
/// Default request buffer size.
const DEFAULT_REQUEST_BUFFER_SIZE: usize = 16;
/// Default exponential backoff base delay.
const DEFAULT_EXPONENTIAL_BACKOFF_BASE_MS: u64 = 1000;
/// Default exponential backoff maximum delay.
const DEFAULT_EXPONENTIAL_BACKOFF_MAX_MS: u64 = 64_000;
/// Default exponential backoff coefficient.
const DEFAULT_EXPONENTIAL_BACKOFF_COEFFICIENT: u64 = 2;

/// Node client configuration.
#[derive(Clone, DataSize, Debug, Deserialize, PartialEq, Eq)]
// Disallow unknown fields to ensure config files and command-line overrides contain valid keys.
#[serde(deny_unknown_fields)]
pub struct NodeClientConfig {
    /// Address of the node.
    pub address: SocketAddr,
    /// Maximum size of a request in bytes.
    pub max_request_size_bytes: u32,
    /// Maximum size of a response in bytes.
    pub max_response_size_bytes: u32,
    /// Maximum number of in-flight node requests.
    pub request_limit: u16,
    /// Number of node requests that can be buffered.
    pub request_buffer_size: usize,
    /// Configuration for exponential backoff to be used for re-connects.
    pub exponential_backoff: ExponentialBackoffConfig,
}

impl NodeClientConfig {
    /// Creates a default instance for `NodeClientConfig`.
    pub fn new() -> Self {
        NodeClientConfig {
            address: DEFAULT_NODE_CONNECT_ADDRESS.into(),
            request_limit: DEFAULT_NODE_REQUEST_LIMIT,
            max_request_size_bytes: DEFAULT_MAX_NODE_PAYLOAD_SIZE,
            max_response_size_bytes: DEFAULT_MAX_NODE_PAYLOAD_SIZE,
            request_buffer_size: DEFAULT_REQUEST_BUFFER_SIZE,
            exponential_backoff: ExponentialBackoffConfig {
                initial_delay_ms: DEFAULT_EXPONENTIAL_BACKOFF_BASE_MS,
                max_delay_ms: DEFAULT_EXPONENTIAL_BACKOFF_MAX_MS,
                coefficient: DEFAULT_EXPONENTIAL_BACKOFF_COEFFICIENT,
                max_attempts: MaxAttempts::Infinite,
            },
        }
    }

    #[cfg(test)]
    pub fn finite_retries_config(port: u16, num_of_retries: usize) -> Self {
        let local_socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
        NodeClientConfig {
            address: local_socket,
            request_limit: DEFAULT_NODE_REQUEST_LIMIT,
            max_request_size_bytes: DEFAULT_MAX_NODE_PAYLOAD_SIZE,
            max_response_size_bytes: DEFAULT_MAX_NODE_PAYLOAD_SIZE,
            request_buffer_size: DEFAULT_REQUEST_BUFFER_SIZE,
            exponential_backoff: ExponentialBackoffConfig {
                initial_delay_ms: 500,
                max_delay_ms: 3000,
                coefficient: 3,
                max_attempts: MaxAttempts::Finite(num_of_retries),
            },
        }
    }
}

impl Default for NodeClientConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Node client configuration.
#[derive(Clone, DataSize, Debug, Deserialize, PartialEq, Eq)]
// Disallow unknown fields to ensure config files and command-line overrides contain valid keys.
#[serde(deny_unknown_fields)]
pub struct NodeClientConfigTarget {
    /// Address of the node.
    pub address: SocketAddr,
    /// Maximum size of a request in bytes.
    pub max_request_size_bytes: u32,
    /// Maximum size of a response in bytes.
    pub max_response_size_bytes: u32,
    /// Maximum number of in-flight node requests.
    pub request_limit: u16,
    /// Number of node requests that can be buffered.
    pub request_buffer_size: usize,
    /// Configuration for exponential backoff to be used for re-connects.
    pub exponential_backoff: ExponentialBackoffConfigTarget,
}

impl TryFrom<NodeClientConfigTarget> for NodeClientConfig {
    type Error = FieldParseError;
    fn try_from(value: NodeClientConfigTarget) -> Result<Self, Self::Error> {
        let exponential_backoff =
            value
                .exponential_backoff
                .try_into()
                .map_err(|e: FieldParseError| FieldParseError::ParseError {
                    field_name: "exponential_backoff",
                    error: e.to_string(),
                })?;
        Ok(NodeClientConfig {
            address: value.address,
            request_limit: value.request_limit,
            max_request_size_bytes: value.max_request_size_bytes,
            max_response_size_bytes: value.max_response_size_bytes,
            request_buffer_size: value.request_buffer_size,
            exponential_backoff,
        })
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
    pub max_attempts: MaxAttempts,
}

/// Exponential backoff configuration for re-connects.
#[derive(Clone, DataSize, Debug, Deserialize, PartialEq, Eq)]
// Disallow unknown fields to ensure config files and command-line overrides contain valid keys.
#[serde(deny_unknown_fields)]
pub struct ExponentialBackoffConfigTarget {
    /// Initial wait time before the first re-connect attempt.
    pub initial_delay_ms: u64,
    /// Maximum wait time between re-connect attempts.
    pub max_delay_ms: u64,
    /// The multiplier to apply to the previous delay to get the next delay.
    pub coefficient: u64,
    /// Maximum number of re-connect attempts.
    pub max_attempts: MaxAttemptsTarget,
}

impl TryFrom<ExponentialBackoffConfigTarget> for ExponentialBackoffConfig {
    type Error = FieldParseError;
    fn try_from(value: ExponentialBackoffConfigTarget) -> Result<Self, Self::Error> {
        let max_attempts = value
            .max_attempts
            .try_into()
            .map_err(|e: MaxAttemptsError| FieldParseError::ParseError {
                field_name: "max_attempts",
                error: e.to_string(),
            })?;
        Ok(ExponentialBackoffConfig {
            initial_delay_ms: value.initial_delay_ms,
            max_delay_ms: value.max_delay_ms,
            coefficient: value.coefficient,
            max_attempts,
        })
    }
}

#[derive(Clone, DataSize, Debug, Deserialize, PartialEq, Eq)]
pub enum MaxAttempts {
    Infinite,
    Finite(usize),
}

impl MaxAttempts {
    pub fn can_attempt(&self, current_attempt: usize) -> bool {
        match self {
            MaxAttempts::Infinite => true,
            MaxAttempts::Finite(max_attempts) => *max_attempts >= current_attempt,
        }
    }
}

#[derive(Clone, DataSize, Debug, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum MaxAttemptsTarget {
    StringBased(String),
    UsizeBased(usize),
}

impl TryFrom<MaxAttemptsTarget> for MaxAttempts {
    type Error = MaxAttemptsError;
    fn try_from(value: MaxAttemptsTarget) -> Result<Self, Self::Error> {
        match value {
            MaxAttemptsTarget::StringBased(s) => {
                if s == "infinite" {
                    Ok(MaxAttempts::Infinite)
                } else {
                    Err(MaxAttemptsError::UnexpectedValue(s))
                }
            }
            MaxAttemptsTarget::UsizeBased(u) => {
                if u == 0 {
                    Err(MaxAttemptsError::UnexpectedValue(u.to_string()))
                } else {
                    Ok(MaxAttempts::Finite(u))
                }
            }
        }
    }
}

#[derive(Error, Debug)]
pub enum MaxAttemptsError {
    #[error("Max attempts must be either 'infinite' or a integer > 0. Got: {}", .0)]
    UnexpectedValue(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_should_deserialize_infinite() {
        let json = r#""infinite""#.to_string();
        let deserialized: MaxAttempts = serde_json::from_str::<MaxAttemptsTarget>(&json)
            .unwrap()
            .try_into()
            .unwrap();
        assert_eq!(deserialized, MaxAttempts::Infinite);
    }

    #[test]
    fn test_should_deserialize_finite() {
        let json = r#"125"#.to_string();
        let deserialized: MaxAttempts = serde_json::from_str::<MaxAttemptsTarget>(&json)
            .unwrap()
            .try_into()
            .unwrap();
        assert_eq!(deserialized, MaxAttempts::Finite(125));
    }

    #[test]
    fn test_should_fail_on_other_inputs() {
        assert_failing_deserialization(r#""x""#);
        assert_failing_deserialization(r#""infiniteee""#);
        assert_failing_deserialization(r#""infinite ""#);
        assert_failing_deserialization(r#"" infinite""#);
        let deserialized = serde_json::from_str::<MaxAttemptsTarget>(r#"-1"#);
        assert!(deserialized.is_err());
    }

    fn assert_failing_deserialization(input: &str) {
        let deserialized: Result<MaxAttempts, MaxAttemptsError> =
            serde_json::from_str::<MaxAttemptsTarget>(input)
                .unwrap()
                .try_into();
        assert!(deserialized.is_err(), "input = {}", input);
    }
}
