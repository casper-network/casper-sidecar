mod reader;
mod writer;

use std::env;

use crate::{
    database::migration_manager::MigrationManager,
    sql::tables,
    types::{config::PostgresqlConfig, database::DatabaseWriteError},
};
use anyhow::Error;
use sea_query::PostgresQueryBuilder;
use sqlx::{
    postgres::{PgConnectOptions, PgPool, PgPoolOptions},
    ConnectOptions, Executor, Postgres, Transaction,
};

/// The environment variable key for the Postgres username.
const DATABASE_USERNAME_ENV_VAR_KEY: &str = "SIDECAR_POSTGRES_USERNAME";
/// The environment variable key for the Postgres password.
const DATABASE_PASSWORD_ENV_VAR_KEY: &str = "SIDECAR_POSTGRES_PASSWORD";
/// The environment variable key for the Postgres database name.
const DATABASE_NAME_ENV_VAR_KEY: &str = "SIDECAR_POSTGRES_DATABASE_NAME";
/// The environment variable key for the Postgres host.
const DATABASE_HOST_ENV_VAR_KEY: &str = "SIDECAR_POSTGRES_HOST";
/// The environment variable key for the Postgres max connections.
const DATABASE_MAX_CONNECTIONS_ENV_VAR_KEY: &str = "SIDECAR_POSTGRES_MAX_CONNECTIONS";
/// The environment variable key for the Postgres port.
const DATABASE_PORT_ENV_VAR_KEY: &str = "SIDECAR_POSTGRES_PORT";

/// The default postgres max connections.
const DEFAULT_MAX_CONNECTIONS: u32 = 10;
/// The default postgres port.
const DEFAULT_PORT: u16 = 5432;

/// [PostgreSqlDatabase] can be cloned to allow multiple components access to the database.
/// The [PostgreSqlDatabase] is cloned using an [Arc](std::sync::Arc) so each cloned instance of [PostgreSqlDatabase] shares the same connection pool.
#[derive(Clone)]
pub struct PostgreSqlDatabase {
    pub connection_pool: PgPool,
}

impl PostgreSqlDatabase {
    pub async fn new(config: PostgresqlConfig) -> Result<PostgreSqlDatabase, Error> {
        let host = get_connection_information_from_env(DATABASE_HOST_ENV_VAR_KEY, config.host);
        let database_name =
            get_connection_information_from_env(DATABASE_NAME_ENV_VAR_KEY, config.database_name);
        let database_username = get_connection_information_from_env(
            DATABASE_USERNAME_ENV_VAR_KEY,
            config.database_username,
        );
        let database_password = get_connection_information_from_env(
            DATABASE_PASSWORD_ENV_VAR_KEY,
            config.database_password,
        );

        let max_connections = get_connection_information_from_env(
            DATABASE_MAX_CONNECTIONS_ENV_VAR_KEY,
            config.max_connections_in_pool,
        )
        .parse::<u32>()
        .unwrap_or(DEFAULT_MAX_CONNECTIONS);
        let port: u16 = get_connection_information_from_env(DATABASE_PORT_ENV_VAR_KEY, config.port)
            .parse::<u16>()
            .unwrap_or(DEFAULT_PORT);

        let db_connection_config = PgConnectOptions::new()
            .host(host.as_str())
            .database(database_name.as_str())
            .username(database_username.as_str())
            .password(database_password.as_str())
            .port(port)
            .disable_statement_logging();

        let connection_pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect_lazy_with(db_connection_config);

        let db = PostgreSqlDatabase { connection_pool };

        MigrationManager::apply_all_migrations(db.clone()).await?;

        Ok(db)
    }

    async fn store_version_based_on_result(
        &self,
        maybe_version: Option<u32>,
        res: Result<(), DatabaseWriteError>,
    ) -> Result<(), DatabaseWriteError> {
        let db_connection = &self.connection_pool;
        match maybe_version {
            None => res,
            Some(version) => match res {
                Ok(_) => {
                    let insert_version_stmt = tables::migration::create_insert_stmt(version, true)?
                        .to_string(PostgresQueryBuilder);
                    db_connection.execute(insert_version_stmt.as_str()).await?;
                    Ok(())
                }
                Err(e) => {
                    let insert_version_stmt =
                        tables::migration::create_insert_stmt(version, false)?
                            .to_string(PostgresQueryBuilder);
                    db_connection.execute(insert_version_stmt.as_str()).await?;
                    Err(e)
                }
            },
        }
    }

    pub async fn get_transaction(&self) -> Result<Transaction<Postgres>, sqlx::Error> {
        self.connection_pool.begin().await
    }
}
/// This function will return the value of the environment variable with the key `key`.
///
/// If the environment variable is not set, the function will check if a value has been set in the config file
/// and return that value.
///
/// If no value has been set in the config file, the function will return an empty string.
///
/// Note: The empty string will likely cause an error in setting up the db. This is expected and should
/// happen if the config value is not set in either place.
fn get_connection_information_from_env<T: ToString>(key: &str, config_backup: Option<T>) -> String {
    env::var(key).unwrap_or_else(|_| match config_backup {
        Some(config_value) => config_value.to_string(),
        None => "".to_string(),
    })
}
