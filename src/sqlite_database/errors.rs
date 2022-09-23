use anyhow::Error;

use crate::types::database::DatabaseRequestError;

pub(super) enum SqliteDbError {
    Sqlx(sqlx::Error),
    SerdeJson(serde_json::Error),
}

impl From<sqlx::Error> for SqliteDbError {
    fn from(error: sqlx::Error) -> Self {
        SqliteDbError::Sqlx(error)
    }
}

pub(super) fn wrap_query_error(error: SqliteDbError) -> DatabaseRequestError {
    match error {
        SqliteDbError::Sqlx(err) => match err.to_string().as_str() {
            "Query returned no rows" => DatabaseRequestError::NotFound,
            _ => DatabaseRequestError::Unhandled(Error::from(err)),
        },
        SqliteDbError::SerdeJson(err) => DatabaseRequestError::Serialisation(Error::from(err)),
    }
}
