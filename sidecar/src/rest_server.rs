mod errors;
pub mod filters;
mod handlers;
mod openapi;
#[cfg(test)]
mod tests;
use anyhow::Error;
use hyper::Server;
use std::net::TcpListener;
use std::time::Duration;
use tokio::task::JoinHandle;
use tower::{buffer::Buffer, make::Shared, ServiceBuilder};
use tower_http::auth::AsyncRequireAuthorizationLayer;
use warp::Filter;

use crate::authorization::{
    build_authorization_validator, AuthorizationValidator, HeaderBasedAuthorizeRequest,
};
use crate::types::config::AuthConfig;
use crate::types::database::Database;
use crate::{
    types::{config::RestServerConfig, database::DatabaseReader},
    utils::resolve_address,
};

const BIND_ALL_INTERFACES: &str = "0.0.0.0";

pub fn build_and_start_rest_server(
    rest_server_config: &RestServerConfig,
    auth_config: &Option<AuthConfig>,
    database: Database,
) -> JoinHandle<Result<(), Error>> {
    let rest_server_config = rest_server_config.clone();
    let authorization_validator = build_authorization_validator(auth_config);
    tokio::spawn(async move {
        match database {
            Database::SqliteDatabaseWrapper(db) => {
                run_server(authorization_validator, rest_server_config, db.clone()).await
            }
            Database::PostgreSqlDatabaseWrapper(db) => {
                run_server(authorization_validator, rest_server_config, db.clone()).await
            }
        }
    })
}

async fn run_server<Db: DatabaseReader + Clone + Send + Sync + 'static>(
    authorization_validator: AuthorizationValidator,
    config: RestServerConfig,
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
        );
    match authorization_validator {
        AuthorizationValidator::SimpleAuthorization(authorizer) => {
            let tower_service = tower_service
                .layer(AsyncRequireAuthorizationLayer::new(
                    HeaderBasedAuthorizeRequest::new(authorizer),
                ))
                .service(warp_service);
            Server::from_tcp(listener)?
                .serve(Shared::new(Buffer::new(tower_service, 50)))
                .await?;
        }
        AuthorizationValidator::NoAuthorization => {
            let tower_service = tower_service.service(warp_service);
            Server::from_tcp(listener)?
                .serve(Shared::new(Buffer::new(tower_service, 50)))
                .await?;
        }
    }
    Err(Error::msg("REST server shutting down"))
}
