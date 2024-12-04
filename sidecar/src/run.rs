use crate::component::*;
use crate::config::SidecarConfig;
use anyhow::{anyhow, Context, Error};
use casper_event_sidecar::LazyDatabaseWrapper;
use std::process::ExitCode;
use tokio::signal::unix::{signal, SignalKind};
use tracing::{error, info};

pub async fn run(config: SidecarConfig) -> Result<ExitCode, Error> {
    let maybe_database = config
        .storage
        .as_ref()
        .filter(|storage_config| storage_config.is_enabled())
        .map(|storage_config| LazyDatabaseWrapper::new(storage_config.clone()));
    let mut components: Vec<Box<dyn Component>> = Vec::new();
    let admin_api_component = AdminApiComponent::new();
    components.push(Box::new(admin_api_component));
    let rest_api_component = RestApiComponent::new(maybe_database.clone());
    components.push(Box::new(rest_api_component));
    let sse_server_component = SseServerComponent::new(maybe_database);
    components.push(Box::new(sse_server_component));
    let rpc_api_component = RpcApiComponent::new();
    components.push(Box::new(rpc_api_component));

    // setup signal handler futures to wait for interrupts
    let mut sigterm =
        signal(SignalKind::terminate()).context("Failed to initialize SIGTERM handler")?;
    let mut sigint =
        signal(SignalKind::interrupt()).context("Failed to initialize SIGINT handler")?;

    // run signal handlers and main task
    tokio::select! {
        _ = sigterm.recv() => {
            info!("Received SIGTERM signal. Shutting down...");
            Ok(ExitCode::SUCCESS)
        },
        _ = sigint.recv() => {
            info!("Received SIGINT signal. Shutting down...");
            Ok(ExitCode::SUCCESS)
        },
        res = do_run(config, components) => res.map_err(|component_error| {
            error!("The server has exited with an error: {}", component_error);
            anyhow!(component_error.to_string())
        }),
    }
}

async fn do_run(
    config: SidecarConfig,
    components: Vec<Box<dyn Component>>,
) -> Result<ExitCode, ComponentError> {
    let mut component_futures = Vec::new();
    for component in components.iter() {
        let maybe_future = component.prepare_component_task(&config).await?;
        if let Some(future) = maybe_future {
            component_futures.push(future);
        }
    }
    if component_futures.is_empty() {
        info!("No runnable sidecar components are defined/enabled. Exiting");
        return Ok(ExitCode::SUCCESS);
    }
    futures::future::select_all(component_futures).await.0
}
