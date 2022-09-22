use std::convert::Infallible;

use http::StatusCode;
use serde::Serialize;
use tracing::error;
use warp::{Rejection, Reply};

use crate::types::database::DatabaseRequestError;

#[derive(Serialize)]
struct ApiError {
    code: u16,
    message: String,
}

#[derive(Debug)]
pub(super) struct InvalidPath;
impl warp::reject::Reject for InvalidPath {}

#[derive(Debug)]
pub(super) struct SerializationError(serde_json::error::Error);
impl warp::reject::Reject for SerializationError {}

#[derive(Debug)]
pub(super) struct StorageError(pub(super) DatabaseRequestError);
impl warp::reject::Reject for StorageError {}

pub(super) async fn handle_rejection(err: Rejection) -> Result<impl Reply, Infallible> {
    let code;
    let message;

    if err.is_not_found() {
        code = StatusCode::NOT_FOUND;
        message = String::from("No results");
    } else if let Some(SerializationError(err)) = err.find() {
        code = StatusCode::INTERNAL_SERVER_ERROR;
        message = format!("Serialization Error: {}", err);
    } else if let Some(StorageError(err)) = err.find() {
        match err {
            DatabaseRequestError::NotFound => {
                code = StatusCode::NOT_FOUND;
                message = "Query returned no data".to_string();
            }
            DatabaseRequestError::InvalidParam(err) => {
                code = StatusCode::BAD_REQUEST;
                message = format!("Invalid parameter in query: {}", err)
            }
            DatabaseRequestError::Serialisation(err) => {
                code = StatusCode::INTERNAL_SERVER_ERROR;
                message = format!("Error deserializing returned data: {}", err)
            }
            DatabaseRequestError::Unhandled(err) => {
                code = StatusCode::INTERNAL_SERVER_ERROR;
                message = format!("Unhandled error occurred in storage: {}", err)
            }
        }
    } else if let Some(InvalidPath) = err.find() {
        code = StatusCode::BAD_REQUEST;
        message = String::from("Invalid request path provided");
    } else if err.find::<warp::reject::MethodNotAllowed>().is_some() {
        code = StatusCode::METHOD_NOT_ALLOWED;
        message = "Method not allowed".to_string();
    } else {
        error!("Unhandled REST Server Error: {:?}", err);
        code = StatusCode::INTERNAL_SERVER_ERROR;
        message = "Unhandled error - you've found a bug!".to_string();
    }

    let json = warp::reply::json(&ApiError {
        code: code.as_u16(),
        message,
    });

    Ok(warp::reply::with_status(json, code))
}
