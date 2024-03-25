use anyhow::Error;
use async_trait::async_trait;
use casper_event_sidecar::{run as run_sse_sidecar, run_admin_server, run_rest_server, Database};
use casper_rpc_sidecar::build_rpc_server;
use derive_new::new;
use futures::{future::BoxFuture, FutureExt};
use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    process::ExitCode,
};
use tracing::info;

use crate::config::SidecarConfig;

#[derive(Debug)]
pub enum ComponentError {
    Initialization {
        component_name: String,
        internal_error: Error,
    },
    Runtime {
        component_name: String,
        internal_error: Error,
    },
}

impl ComponentError {
    pub fn runtime_error(component_name: String, error: Error) -> Self {
        ComponentError::Runtime {
            component_name,
            internal_error: error,
        }
    }

    pub fn initialization_error(component_name: String, error: Error) -> Self {
        ComponentError::Initialization {
            component_name,
            internal_error: error,
        }
    }
}

impl Display for ComponentError {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            ComponentError::Initialization {
                component_name,
                internal_error,
            } => write!(
                f,
                "Error initializing component '{}': {}",
                component_name, internal_error
            ),
            ComponentError::Runtime {
                component_name,
                internal_error,
            } => write!(
                f,
                "Error running component '{}': {}",
                component_name, internal_error
            ),
        }
    }
}

/// Abstraction for an individual component of sidecar. The assumption is that this should be
/// a long running task that is spawned into the tokio runtime.
#[async_trait]
pub trait Component {
    fn name(&self) -> String;
    /// Returns a future that represents the task of the running component.
    /// If the return value is Ok(None) it means that the component is disabled (or not configured at all) and should not run.
    async fn prepare_component_task(
        &self,
        config: &SidecarConfig,
    ) -> Result<Option<BoxFuture<'_, Result<ExitCode, ComponentError>>>, ComponentError>;
}

#[derive(new)]
pub struct SseServerComponent {
    maybe_database: Option<Database>,
}

#[async_trait]
impl Component for SseServerComponent {
    async fn prepare_component_task(
        &self,
        config: &SidecarConfig,
    ) -> Result<Option<BoxFuture<'_, Result<ExitCode, ComponentError>>>, ComponentError> {
        if let (Some(storage_config), Some(database), Some(sse_server_config)) =
            (&config.storage, &self.maybe_database, &config.sse_server)
        {
            if sse_server_config.enable_server {
                // If sse server is configured, both storage config and database must be "Some" here. This should be ensured by prior validation.
                let future = run_sse_sidecar(
                    sse_server_config.clone(),
                    database.clone(),
                    storage_config.get_storage_path(),
                )
                .map(|res| res.map_err(|e| ComponentError::runtime_error(self.name(), e)));
                Ok(Some(Box::pin(future)))
            } else {
                info!("SSE server is disabled. Skipping...");
                Ok(None)
            }
        } else {
            info!("SSE server not configured. Skipping");
            Ok(None)
        }
    }

    fn name(&self) -> String {
        "sse_event_server".to_string()
    }
}

#[derive(new)]
pub struct RestApiComponent {
    maybe_database: Option<Database>,
}

#[async_trait]
impl Component for RestApiComponent {
    async fn prepare_component_task(
        &self,
        config: &SidecarConfig,
    ) -> Result<Option<BoxFuture<'_, Result<ExitCode, ComponentError>>>, ComponentError> {
        if let (Some(config), Some(database)) = (&config.rest_api_server, &self.maybe_database) {
            if config.enable_server {
                let future = run_rest_server(config.clone(), database.clone())
                    .map(|res| res.map_err(|e| ComponentError::runtime_error(self.name(), e)));
                Ok(Some(Box::pin(future)))
            } else {
                info!("REST API server is disabled. Skipping...");
                Ok(None)
            }
        } else {
            info!("REST API server not configured. Skipping");
            Ok(None)
        }
    }

    fn name(&self) -> String {
        "rest_api_server".to_string()
    }
}

#[derive(new)]
pub struct AdminApiComponent;

#[async_trait]
impl Component for AdminApiComponent {
    async fn prepare_component_task(
        &self,
        config: &SidecarConfig,
    ) -> Result<Option<BoxFuture<'_, Result<ExitCode, ComponentError>>>, ComponentError> {
        if let Some(config) = &config.admin_api_server {
            if config.enable_server {
                let future = run_admin_server(config.clone())
                    .map(|res| res.map_err(|e| ComponentError::runtime_error(self.name(), e)));
                Ok(Some(Box::pin(future)))
            } else {
                info!("Admin API server is disabled. Skipping.");
                Ok(None)
            }
        } else {
            info!("Admin API server not configured. Skipping.");
            Ok(None)
        }
    }

    fn name(&self) -> String {
        "admin_api_server".to_string()
    }
}

#[derive(new)]
pub struct RpcApiComponent;

#[async_trait]
impl Component for RpcApiComponent {
    async fn prepare_component_task(
        &self,
        config: &SidecarConfig,
    ) -> Result<Option<BoxFuture<'_, Result<ExitCode, ComponentError>>>, ComponentError> {
        if let Some(config) = config.rpc_server.as_ref() {
            let is_main_exec_defined = config.main_server.enable_server;
            let is_speculative_exec_defined = config
                .speculative_exec_server
                .as_ref()
                .map(|x| x.enable_server)
                .unwrap_or(false);
            if !is_main_exec_defined && !is_speculative_exec_defined {
                //There was no main rpc server of speculative exec server configured, we shouldn't bother with proceeding
                info!("RPC API server is disabled. Skipping...");
                return Ok(None);
            }
            if !is_main_exec_defined {
                info!("Main RPC API server is disabled. Only speculative server will be running.");
            }
            if !is_speculative_exec_defined {
                info!("Speculative RPC API server is disabled. Only main RPC API will be running.");
            }
            let res = build_rpc_server(config.clone()).await;
            match res {
                Ok(None) => Ok(None),
                Ok(Some(fut)) => {
                    let future = fut
                        .map(|res| res.map_err(|e| ComponentError::runtime_error(self.name(), e)));
                    Ok(Some(Box::pin(future)))
                }
                Err(err) => Err(ComponentError::initialization_error(self.name(), err)),
            }
        } else {
            info!("RPC API server not configured. Skipping.");
            Ok(None)
        }
    }

    fn name(&self) -> String {
        "rpc_api_server".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SidecarConfig;
    use casper_rpc_sidecar::{
        testing::{get_port, start_mock_binary_port_responding_with_stored_value},
        NodeClientConfig, RpcServerConfig, SpeculativeExecConfig,
    };

    #[tokio::test]
    async fn given_sse_server_component_when_no_db_should_return_none() {
        let component = SseServerComponent::new(None);
        let config = all_components_all_enabled();
        let res = component.prepare_component_task(&config).await;
        assert!(res.is_ok());
        assert!(res.unwrap().is_none());
    }

    #[tokio::test]
    async fn given_sse_server_component_when_db_but_no_config_should_return_none() {
        let component = SseServerComponent::new(Some(Database::for_tests()));
        let mut config = all_components_all_disabled();
        config.sse_server = None;
        let res = component.prepare_component_task(&config).await;
        assert!(res.is_ok());
        assert!(res.unwrap().is_none());
    }

    #[tokio::test]
    async fn given_sse_server_component_when_config_disabled_should_return_none() {
        let component = SseServerComponent::new(Some(Database::for_tests()));
        let config = all_components_all_disabled();
        let res = component.prepare_component_task(&config).await;
        assert!(res.is_ok());
        assert!(res.unwrap().is_none());
    }

    #[tokio::test]
    async fn given_sse_server_component_when_db_and_config_should_return_some() {
        let component = SseServerComponent::new(Some(Database::for_tests()));
        let config = all_components_all_enabled();
        let res = component.prepare_component_task(&config).await;
        assert!(res.is_ok());
        assert!(res.unwrap().is_some());
    }

    #[tokio::test]
    async fn given_rest_api_server_component_when_no_db_should_return_none() {
        let component = RestApiComponent::new(None);
        let config = all_components_all_enabled();
        let res = component.prepare_component_task(&config).await;
        assert!(res.is_ok());
        assert!(res.unwrap().is_none());
    }

    #[tokio::test]
    async fn given_rest_api_server_component_when_db_but_no_config_should_return_none() {
        let component = RestApiComponent::new(Some(Database::for_tests()));
        let mut config = all_components_all_disabled();
        config.rest_api_server = None;
        let res = component.prepare_component_task(&config).await;
        assert!(res.is_ok());
        assert!(res.unwrap().is_none());
    }

    #[tokio::test]
    async fn given_rest_api_server_component_when_config_disabled_should_return_none() {
        let component = RestApiComponent::new(Some(Database::for_tests()));
        let config = all_components_all_disabled();
        let res = component.prepare_component_task(&config).await;
        assert!(res.is_ok());
        assert!(res.unwrap().is_none());
    }

    #[tokio::test]
    async fn given_rest_api_server_component_when_db_and_config_should_return_some() {
        let component = RestApiComponent::new(Some(Database::for_tests()));
        let config = all_components_all_enabled();
        let res = component.prepare_component_task(&config).await;
        assert!(res.is_ok());
        assert!(res.unwrap().is_some());
    }

    #[tokio::test]
    async fn given_admin_api_server_component_when_no_config_should_return_none() {
        let component = AdminApiComponent::new();
        let mut config = all_components_all_disabled();
        config.admin_api_server = None;
        let res = component.prepare_component_task(&config).await;
        assert!(res.is_ok());
        assert!(res.unwrap().is_none());
    }

    #[tokio::test]
    async fn given_admin_api_server_component_when_config_disabled_should_return_none() {
        let component = AdminApiComponent::new();
        let config = all_components_all_disabled();
        let res = component.prepare_component_task(&config).await;
        assert!(res.is_ok());
        assert!(res.unwrap().is_none());
    }

    #[tokio::test]
    async fn given_admin_api_server_component_when_config_should_return_some() {
        let component = AdminApiComponent::new();
        let config = all_components_all_enabled();
        let res = component.prepare_component_task(&config).await;
        assert!(res.is_ok());
        assert!(res.unwrap().is_some());
    }

    #[tokio::test]
    async fn given_rpc_api_server_component_when_config_disabled_should_return_none() {
        let component = RpcApiComponent::new();
        let config = all_components_all_disabled();
        let res = component.prepare_component_task(&config).await;
        assert!(res.is_ok());
        assert!(res.unwrap().is_none());
    }

    #[tokio::test]
    async fn given_rpc_api_server_component_when_config_should_return_some() {
        let port = get_port();
        let _mock_server_handle = start_mock_binary_port_responding_with_stored_value(port).await;
        let component = RpcApiComponent::new();
        let mut config = all_components_all_enabled();
        config.rpc_server.as_mut().unwrap().node_client =
            NodeClientConfig::finite_retries_config(port, 1);
        config.rpc_server.as_mut().unwrap().main_server.address = format!("0.0.0.0:{}", port);
        config
            .rpc_server
            .as_mut()
            .unwrap()
            .speculative_exec_server
            .as_mut()
            .unwrap()
            .address = format!("0.0.0.0:{}", port);
        let res = component.prepare_component_task(&config).await;
        assert!(res.is_ok());
        assert!(res.unwrap().is_some());
    }

    #[tokio::test]
    async fn given_rpc_api_server_component_when_no_config_should_return_none() {
        let component = RpcApiComponent::new();
        let mut config = all_components_all_disabled();
        config.rpc_server = None;
        let res = component.prepare_component_task(&config).await;
        assert!(res.is_ok());
        assert!(res.unwrap().is_none());
    }

    fn all_components_all_enabled() -> SidecarConfig {
        let mut rpc_server = RpcServerConfig::default();
        let speculative_config = SpeculativeExecConfig {
            enable_server: true,
            ..Default::default()
        };
        rpc_server.speculative_exec_server = Some(speculative_config);
        SidecarConfig {
            storage: Some(Default::default()),
            admin_api_server: Some(Default::default()),
            rest_api_server: Some(Default::default()),
            sse_server: Some(Default::default()),
            rpc_server: Some(rpc_server),
            ..Default::default()
        }
    }

    fn all_components_all_disabled() -> SidecarConfig {
        let mut config = all_components_all_enabled();
        config.admin_api_server.as_mut().unwrap().enable_server = false;
        config.rest_api_server.as_mut().unwrap().enable_server = false;
        config
            .rpc_server
            .as_mut()
            .unwrap()
            .main_server
            .enable_server = false;
        config
            .rpc_server
            .as_mut()
            .unwrap()
            .speculative_exec_server
            .as_mut()
            .unwrap()
            .enable_server = false;
        config.sse_server.as_mut().unwrap().enable_server = false;
        config
    }
}
