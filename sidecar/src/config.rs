use casper_event_sidecar::{
    DatabaseConfigError, SseEventServerConfig, SseEventServerConfigSerdeTarget,
};
use casper_rpc_sidecar::RpcServerConfig;
use serde::Deserialize;
use thiserror::Error;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct SidecarConfigTarget {
    max_thread_count: Option<usize>,
    max_blocking_thread_count: Option<usize>,
    sse_server: Option<SseEventServerConfigSerdeTarget>,
    rpc_server: Option<RpcServerConfig>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(Default))]
pub struct SidecarConfig {
    pub max_thread_count: Option<usize>,
    pub max_blocking_thread_count: Option<usize>,
    pub sse_server: Option<SseEventServerConfig>,
    pub rpc_server: Option<RpcServerConfig>,
}

impl SidecarConfig {
    pub fn validate(&self) -> Result<(), anyhow::Error> {
        if self.rpc_server.is_none() && self.sse_server.is_none() {
            return Err(anyhow::anyhow!(
                "At least one of RPC server or SSE server must be configured"
            ));
        }
        Ok(())
    }
}

impl TryFrom<SidecarConfigTarget> for SidecarConfig {
    type Error = ConfigReadError;

    fn try_from(value: SidecarConfigTarget) -> Result<Self, Self::Error> {
        let sse_server_config_res: Option<Result<SseEventServerConfig, DatabaseConfigError>> =
            value.sse_server.map(|target| target.try_into());
        let sse_server_config = invert(sse_server_config_res)?;
        Ok(SidecarConfig {
            max_thread_count: value.max_thread_count,
            max_blocking_thread_count: value.max_blocking_thread_count,
            sse_server: sse_server_config,
            rpc_server: value.rpc_server,
        })
    }
}

fn invert<T, E>(x: Option<Result<T, E>>) -> Result<Option<T>, E> {
    x.map_or(Ok(None), |v| v.map(Some))
}

#[derive(Error, Debug)]
pub enum ConfigReadError {
    #[error("failed to read sidecar configuration. Underlying reason: {}", .error)]
    GeneralError { error: String },
}

impl From<DatabaseConfigError> for ConfigReadError {
    fn from(value: DatabaseConfigError) -> Self {
        ConfigReadError::GeneralError {
            error: value.to_string(),
        }
    }
}
