use super::PostgreSqlDatabase;
use crate::{database::types::DDLConfiguration, database_writer_implementation};
use sea_query::PostgresQueryBuilder;
#[cfg(test)]
use sqlx::postgres::PgRow;
use sqlx::{postgres::PgQueryResult, Postgres};
database_writer_implementation!(
    PostgreSqlDatabase,
    Postgres,
    PgQueryResult,
    PostgresQueryBuilder,
    DDLConfiguration {
        db_supports_unsigned: false,
    }
);

#[cfg(test)]
impl PostgreSqlDatabase {
    pub async fn fetch_one(&self, sql: &str) -> PgRow {
        self.connection_pool
            .fetch_one(sql)
            .await
            .expect("Error executing provided SQL")
    }
}
