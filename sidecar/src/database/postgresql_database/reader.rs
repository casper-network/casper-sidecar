use super::PostgreSqlDatabase;
use crate::database_reader_implementation;
use sea_query::PostgresQueryBuilder;
use sqlx::{postgres::PgRow, PgPool};

async fn fetch_optional_with_error_check(
    connection: &PgPool,
    stmt: String,
) -> Result<PgRow, DatabaseReadError> {
    connection
        .fetch_optional(stmt.as_str())
        .await
        .map_err(|sql_err| DatabaseReadError::Unhandled(Error::from(sql_err)))
        .and_then(|maybe_row| match maybe_row {
            None => Err(DatabaseReadError::NotFound),
            Some(row) => Ok(row),
        })
}

fn parse_migration_row(maybe_row: Option<PgRow>) -> Result<Option<(u32, bool)>, DatabaseReadError> {
    match maybe_row {
        None => Ok(None),
        Some(row) => {
            let version = row
                .try_get::<i32, &str>("version")
                .map_err(|err| wrap_query_error(err.into()))?;
            let is_success = row
                .try_get::<bool, &str>("is_success")
                .map_err(|err| wrap_query_error(err.into()))?;
            Ok(Some((version as u32, is_success)))
        }
    }
}

database_reader_implementation!(PostgreSqlDatabase, PgPool, PgRow, PostgresQueryBuilder {});
