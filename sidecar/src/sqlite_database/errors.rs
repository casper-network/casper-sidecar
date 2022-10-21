use anyhow::Error;

use crate::types::database::DatabaseReadError;

pub(super) enum SqliteDbError {
    Sqlx(sqlx::Error),
    SerdeJson(serde_json::Error),
    Decompression(miniz_oxide::inflate::DecompressError),
}

impl From<sqlx::Error> for SqliteDbError {
    fn from(error: sqlx::Error) -> Self {
        SqliteDbError::Sqlx(error)
    }
}

impl From<serde_json::Error> for SqliteDbError {
    fn from(error: serde_json::Error) -> Self {
        SqliteDbError::SerdeJson(error)
    }
}

impl From<miniz_oxide::inflate::DecompressError> for SqliteDbError {
    fn from(error: miniz_oxide::inflate::DecompressError) -> Self {
        SqliteDbError::Decompression(error)
    }
}

pub(super) fn wrap_query_error(error: SqliteDbError) -> DatabaseReadError {
    match error {
        SqliteDbError::Sqlx(err) => match err.to_string().as_str() {
            "Query returned no rows" => DatabaseReadError::NotFound,
            _ => DatabaseReadError::Unhandled(Error::from(err)),
        },
        SqliteDbError::SerdeJson(serde_err) => DatabaseReadError::Serialisation(serde_err),
        SqliteDbError::Decompression(decompression_err) => {
            DatabaseReadError::Compression(decompression_err)
        }
    }
}
