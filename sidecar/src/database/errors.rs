use crate::types::database::DatabaseReadError;
use anyhow::Error;

pub enum DbError {
    Sqlx(sqlx::Error),
    SerdeJson(serde_json::Error),
}

impl From<sqlx::Error> for DbError {
    fn from(error: sqlx::Error) -> Self {
        DbError::Sqlx(error)
    }
}

pub fn wrap_query_error(error: DbError) -> DatabaseReadError {
    match error {
        DbError::Sqlx(err) => match err.to_string().as_str() {
            "Query returned no rows" => DatabaseReadError::NotFound,
            _ => DatabaseReadError::Unhandled(Error::from(err)),
        },
        DbError::SerdeJson(serde_err) => DatabaseReadError::Serialisation(serde_err),
    }
}
