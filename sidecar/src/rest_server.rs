mod errors;
mod filters;
mod handlers;
#[cfg(test)]
mod tests;

use std::net::TcpListener;
use std::time::Duration;

use anyhow::Error;
use hyper::Server;
use tower::{buffer::Buffer, make::Shared, ServiceBuilder};

use crate::{
    sqlite_database::SqliteDatabase, types::config::RestServerConfig, utils::resolve_address,
};

const LOOPBACK: &str = "127.0.0.1";

pub async fn run_server(
    config: RestServerConfig,
    sqlite_database: SqliteDatabase,
) -> Result<(), Error> {
    let api = filters::combined_filters(sqlite_database);

    let address = format!("{}:{}", LOOPBACK, config.port);
    let socket_address = resolve_address(&address)?;

    let listener = TcpListener::bind(socket_address)?;

    let warp_service = warp::service(api);
    let tower_service = ServiceBuilder::new()
        .concurrency_limit(config.max_concurrent_requests as usize)
        .rate_limit(
            config.max_requests_per_second as u64,
            Duration::from_secs(1),
        )
        .service(warp_service);

    Server::from_tcp(listener)?
        .serve(Shared::new(Buffer::new(tower_service, 50)))
        .await?;

    Err(Error::msg("REST server shutting down"))
}
