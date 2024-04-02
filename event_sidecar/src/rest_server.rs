mod errors;
pub mod filters;
mod handlers;
mod metrics_layer;
mod openapi;
mod status;
#[cfg(test)]
mod tests;

use anyhow::Error;
use hyper::Server;
use std::net::TcpListener;
use std::time::Duration;
use tower::{buffer::Buffer, make::Shared, ServiceBuilder};
use tracing::info;
use warp::Filter;

use crate::{
    types::{config::RestApiServerConfig, database::DatabaseReader},
    utils::resolve_address,
};

use self::metrics_layer::MetricsLayer;

const BIND_ALL_INTERFACES: &str = "0.0.0.0";

pub async fn run_server<Db: DatabaseReader + Clone + Send + Sync + 'static>(
    config: RestApiServerConfig,
    database: Db,
) -> Result<(), Error> {
    let api = filters::combined_filters(database);
    let address = format!("{}:{}", BIND_ALL_INTERFACES, config.port);
    let socket_address = resolve_address(&address)?;

    let listener = TcpListener::bind(socket_address)?;

    let warp_service = warp::service(api.with(warp::cors().allow_any_origin()));
    let tower_service = ServiceBuilder::new()
        .concurrency_limit(config.max_concurrent_requests as usize)
        .rate_limit(
            config.max_requests_per_second as u64,
            Duration::from_secs(1),
        )
        .layer(MetricsLayer::new(path_abstraction_for_metrics))
        .service(warp_service);
    info!(address = %address, "started {} server", "REST API");
    Server::from_tcp(listener)?
        .serve(Shared::new(Buffer::new(tower_service, 50)))
        .await?;

    Err(Error::msg("REST server shutting down"))
}

pub(super) fn path_abstraction_for_metrics(path: &str) -> String {
    let parts = path
        .split('/')
        .filter(|el| !el.is_empty())
        .collect::<Vec<&str>>();
    if parts.is_empty() {
        return "unknown".to_string();
    }
    if parts[0] == "block" {
        return "/block/(...)".to_string();
    }
    if parts[0] == "step" {
        return "/step/(...)".to_string();
    }
    if parts[0] == "faults" {
        return "/faults/(...)".to_string();
    }
    if parts[0] == "signatures" {
        return "/signatures/(...)".to_string();
    }
    if parts[0] == "transaction" {
        if parts.len() == 3 {
            // paths like /transaction/deploy/<hash>
            return "/transaction/(...)".to_string();
        }
        if parts.len() == 4 {
            // paths like /transaction/accepted/deploy/<hash>
            return format!("/transaction/{}/(...)", parts[1]);
        }
    }
    if parts[0] == "status" {
        return "/status".to_string();
    }
    "unknown".to_string()
}
