use anyhow::Error;

use crate::types::database::DatabaseReadError;

pub(super) enum SqliteDbError {
    Sqlx(sqlx::Error),
    SerdeJson(serde_json::Error),
}

impl From<sqlx::Error> for SqliteDbError {
    fn from(error: sqlx::Error) -> Self {
        SqliteDbError::Sqlx(error)
    }
}

pub(super) fn wrap_query_error(error: SqliteDbError) -> DatabaseReadError {
    match error {
        SqliteDbError::Sqlx(err) => {
            println!("SQLX error: {:?}", err);
            match err.to_string().as_str() {
                "Query returned no rows" => DatabaseReadError::NotFound,
                _ => DatabaseReadError::Unhandled(Error::from(err)),
            }
        }
        SqliteDbError::SerdeJson(err) => DatabaseReadError::Serialisation(err),
    }
}
