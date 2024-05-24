use anyhow::bail;
use casper_event_sidecar::{
    AdminApiServerConfig, DatabaseConfigError, RestApiServerConfig, SseEventServerConfig,
    StorageConfig, StorageConfigSerdeTarget,
};
use casper_rpc_sidecar::{FieldParseError, RpcServerConfig, RpcServerConfigTarget};
use serde::Deserialize;
use thiserror::Error;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct SidecarConfigTarget {
    max_thread_count: Option<usize>,
    max_blocking_thread_count: Option<usize>,
    storage: Option<StorageConfigSerdeTarget>,
    rest_api_server: Option<RestApiServerConfig>,
    admin_api_server: Option<AdminApiServerConfig>,
    sse_server: Option<SseEventServerConfig>,
    rpc_server: Option<RpcServerConfigTarget>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(Default))]
pub struct SidecarConfig {
    pub max_thread_count: Option<usize>,
    pub max_blocking_thread_count: Option<usize>,
    pub sse_server: Option<SseEventServerConfig>,
    pub rpc_server: Option<RpcServerConfig>,
    pub storage: Option<StorageConfig>,
    pub rest_api_server: Option<RestApiServerConfig>,
    pub admin_api_server: Option<AdminApiServerConfig>,
}

impl SidecarConfig {
    pub fn validate(&self) -> Result<(), anyhow::Error> {
        if !self.is_rpc_server_enabled() && !self.is_sse_server_enabled() {
            bail!("At least one of RPC server or SSE server must be configured")
        }
        let is_storage_enabled = self.is_storage_enabled();
        let is_rest_api_server_enabled = self.is_rest_api_server_enabled();
        let is_sse_storing_events = self.is_sse_storing_events();
        if !is_storage_enabled && is_sse_storing_events {
            bail!("Can't run SSE with events persistence enabled without storage defined")
        }
        if !is_storage_enabled && is_rest_api_server_enabled {
            bail!("Can't run Rest api server without storage defined")
        }
        if !is_sse_storing_events && is_rest_api_server_enabled {
            bail!("Can't run Rest api server with SSE events persistence disabled")
        }
        Ok(())
    }

    fn is_storage_enabled(&self) -> bool {
        self.storage.is_some() && self.storage.as_ref().unwrap().is_enabled()
    }

    fn is_rpc_server_enabled(&self) -> bool {
        self.rpc_server.is_some() && self.rpc_server.as_ref().unwrap().main_server.enable_server
    }

    fn is_sse_server_enabled(&self) -> bool {
        self.sse_server.is_some() && self.sse_server.as_ref().unwrap().enable_server
    }

    fn is_sse_storing_events(&self) -> bool {
        self.is_sse_server_enabled()
            && !self
                .sse_server
                .as_ref()
                .unwrap()
                .is_event_persistence_disabled()
    }

    fn is_rest_api_server_enabled(&self) -> bool {
        self.rest_api_server.is_some() && self.rest_api_server.as_ref().unwrap().enable_server
    }
}

impl TryFrom<SidecarConfigTarget> for SidecarConfig {
    type Error = ConfigReadError;

    fn try_from(value: SidecarConfigTarget) -> Result<Self, Self::Error> {
        let sse_server_config = value.sse_server;
        let storage_config_res: Option<Result<StorageConfig, DatabaseConfigError>> =
            value.storage.map(|target| target.try_into());
        let storage_config = invert(storage_config_res)?;
        let rpc_server_config_res: Option<Result<RpcServerConfig, FieldParseError>> =
            value.rpc_server.map(|target| target.try_into());
        let rpc_server_config = invert(rpc_server_config_res)?;
        Ok(SidecarConfig {
            max_thread_count: value.max_thread_count,
            max_blocking_thread_count: value.max_blocking_thread_count,
            sse_server: sse_server_config,
            rpc_server: rpc_server_config,
            storage: storage_config,
            rest_api_server: value.rest_api_server,
            admin_api_server: value.admin_api_server,
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

impl From<FieldParseError> for ConfigReadError {
    fn from(value: FieldParseError) -> Self {
        ConfigReadError::GeneralError {
            error: value.to_string(),
        }
    }
}

impl From<DatabaseConfigError> for ConfigReadError {
    fn from(value: DatabaseConfigError) -> Self {
        ConfigReadError::GeneralError {
            error: value.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sidecar_config_should_fail_validation_when_sse_server_and_no_storage() {
        let config = SidecarConfig {
            sse_server: Some(SseEventServerConfig::default()),
            ..Default::default()
        };

        let res = config.validate();

        assert!(res.is_err());
        let error_message = res.err().unwrap().to_string();
        assert!(error_message
            .contains("Can't run SSE with events persistence enabled without storage defined"));
    }

    #[test]
    fn sidecar_config_should_fail_validation_when_rest_api_server_and_no_storage() {
        let config = SidecarConfig {
            rpc_server: Some(RpcServerConfig::default()),
            sse_server: None,
            rest_api_server: Some(RestApiServerConfig::default()),
            ..Default::default()
        };

        let res = config.validate();

        assert!(res.is_err());
        let error_message = res.err().unwrap().to_string();
        println!("{}", error_message);
        assert!(error_message.contains("Can't run Rest api server without storage defined"));
    }

    #[test]
    fn sidecar_config_should_fail_validation_when_rest_api_and_sse_has_no_persistence() {
        let sse_server = SseEventServerConfig::default_no_persistence();
        let config = SidecarConfig {
            rpc_server: Some(RpcServerConfig::default()),
            sse_server: Some(sse_server),
            rest_api_server: Some(RestApiServerConfig::default()),
            storage: Some(StorageConfig::default()),
            ..Default::default()
        };

        let res = config.validate();

        assert!(res.is_err());
        let error_message = res.err().unwrap().to_string();
        assert!(error_message
            .contains("Can't run Rest api server with SSE events persistence disabled"));
    }

    #[test]
    fn sidecar_config_should_be_ok_if_rpc_is_defined_and_nothing_else() {
        let config = SidecarConfig {
            rpc_server: Some(RpcServerConfig::default()),
            ..Default::default()
        };

        let res = config.validate();

        assert!(res.is_ok());
    }
}
