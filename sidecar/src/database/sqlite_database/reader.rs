use crate::database_reader_implementation;

use super::SqliteDatabase;
use sea_query::SqliteQueryBuilder;
use sqlx::{sqlite::SqliteRow, SqlitePool};

async fn fetch_optional_with_error_check(
    connection: &SqlitePool,
    stmt: String,
) -> Result<SqliteRow, DatabaseReadError> {
    connection
        .fetch_optional(stmt.as_str())
        .await
        .map_err(|sql_err| DatabaseReadError::Unhandled(Error::from(sql_err)))
        .and_then(|maybe_row| match maybe_row {
            None => Err(DatabaseReadError::NotFound),
            Some(row) => Ok(row),
        })
}

fn parse_migration_row(
    maybe_row: Option<SqliteRow>,
) -> Result<Option<(u32, bool)>, DatabaseReadError> {
    match maybe_row {
        None => Ok(None),
        Some(row) => {
            let version = row
                .try_get::<u32, &str>("version")
                .map_err(|err| wrap_query_error(err.into()))?;
            let is_success = row
                .try_get::<bool, &str>("is_success")
                .map_err(|err| wrap_query_error(err.into()))?;
            Ok(Some((version, is_success)))
        }
    }
}

database_reader_implementation!(SqliteDatabase, SqlitePool, SqliteRow, SqliteQueryBuilder {});
