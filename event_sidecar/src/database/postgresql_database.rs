mod reader;
#[cfg(test)]
mod tests;
mod writer;
use anyhow::Error;
use sea_query::PostgresQueryBuilder;
use sqlx::{
    postgres::{PgConnectOptions, PgPool, PgPoolOptions},
    ConnectOptions, Executor, Postgres, Transaction,
};

use crate::{
    database::migration_manager::MigrationManager,
    sql::tables,
    types::{config::PostgresqlConfig, database::DatabaseWriteError},
};

/// [PostgreSqlDatabase] can be cloned to allow multiple components access to the database.
/// The [PostgreSqlDatabase] is cloned using an [Arc](std::sync::Arc) so each cloned instance of [PostgreSqlDatabase] shares the same connection pool.
#[derive(Clone)]
pub struct PostgreSqlDatabase {
    pub connection_pool: PgPool,
}

impl PostgreSqlDatabase {
    #[cfg(test)]
    pub async fn new_from_postgres_uri(uri: String) -> Result<PostgreSqlDatabase, Error> {
        let connection_pool = PgPoolOptions::new()
            .max_connections(30)
            .connect(uri.as_str())
            .await?;
        let db = PostgreSqlDatabase { connection_pool };
        MigrationManager::apply_all_migrations(db.clone()).await?;
        Ok(db)
    }

    pub async fn new(config: PostgresqlConfig) -> Result<PostgreSqlDatabase, Error> {
        let host = config.host;
        let database_name = config.database_name;
        let database_username = config.database_username;
        let database_password = config.database_password;
        let port = config.port;
        let max_connections = config.max_connections_in_pool;

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
