use std::convert::Infallible;

use crate::{
    types::database::DatabaseReadError,
    utils::{InvalidPath, Unexpected},
};
use http::StatusCode;
#[cfg(test)]
use hyper::body::HttpBody;
use serde::{Deserialize, Serialize};
use tracing::error;
use warp::{reject, Rejection, Reply};

#[derive(Deserialize, Serialize)]
struct ApiError {
    code: u16,
    message: String,
}

#[derive(Debug)]
pub(super) struct InvalidParam(pub(super) anyhow::Error);
impl reject::Reject for InvalidParam {}

#[derive(Debug)]
pub(super) struct StorageError(pub(super) DatabaseReadError);
impl reject::Reject for StorageError {}

/// Handle various REST server errors:
/// - Unexpected internal server errors
/// - Queries returning empty result sets
/// - Serialization errors
/// - Database-related errors
/// - Invalid request path errors
/// - Invalid parameters in the request query
pub(super) async fn handle_rejection(err: Rejection) -> Result<impl Reply, Infallible> {
    let code;
    let message;

    if let Some(Unexpected(err)) = err.find() {
        let err_msg = format!(
            "Unexpected error in REST server - please file a bug report!\n{}",
            err
        );
        error!(%err_msg);
        code = StatusCode::INTERNAL_SERVER_ERROR;
        message = err_msg;
    } else if let Some(StorageError(err)) = err.find() {
        match err {
            DatabaseReadError::NotFound => {
                code = StatusCode::NOT_FOUND;
                message = "Query returned no results".to_string();
            }
            DatabaseReadError::Serialisation(err) => {
                code = StatusCode::INTERNAL_SERVER_ERROR;
                message = format!("Error deserializing returned data: {}", err)
            }
            DatabaseReadError::Unhandled(err) => {
                code = StatusCode::INTERNAL_SERVER_ERROR;
                message = format!("Unhandled error occurred in storage: {}", err)
            }
        }
    } else if let Some(InvalidPath) = err.find() {
        code = StatusCode::BAD_REQUEST;
        message = "Invalid request path provided".to_string();
    } else if let Some(InvalidParam(err)) = err.find() {
        code = StatusCode::BAD_REQUEST;
        message = format!("Invalid parameter in query: {}", err);
    } else {
        let err_msg = format!(
            "Unexpected error in REST server - please file a bug report!\n{:?}",
            err
        );
        error!(%err_msg);
        code = StatusCode::INTERNAL_SERVER_ERROR;
        message = err_msg;
    }

    let json = warp::reply::json(&ApiError {
        code: code.as_u16(),
        message,
    });

    Ok(warp::reply::with_status(json, code))
}

#[cfg(test)]
async fn get_api_error_from_rejection(rejection: Rejection) -> ApiError {
    let response = handle_rejection(rejection)
        .await
        .expect("Rejection handling should not have failed")
        .into_response();

    let err_bytes = response
        .into_body()
        .data()
        .await
        .expect("Body was missing API Error")
        .expect("Body contained an Err value instead of Ok(ApiError)");

    serde_json::from_slice::<ApiError>(&err_bytes)
        .expect("Error parsing ApiError from bytes of body")
}

#[tokio::test]
async fn should_handle_invalid_path() {
    let rejection = reject::custom(InvalidPath);

    let api_error = get_api_error_from_rejection(rejection).await;

    assert_eq!(api_error.code, 400);
    assert_eq!(api_error.message, "Invalid request path provided");
}

#[tokio::test]
async fn should_handle_invalid_param() {
    let rejection = reject::custom(InvalidParam(anyhow::Error::msg("Invalid param provided")));

    let api_error = get_api_error_from_rejection(rejection).await;

    assert_eq!(api_error.code, 400);
    assert!(api_error.message.contains("Invalid parameter in query"));
}

#[tokio::test]
async fn should_handle_not_found() {
    let rejection = reject::custom(StorageError(DatabaseReadError::NotFound));

    let api_error = get_api_error_from_rejection(rejection).await;

    assert_eq!(api_error.code, 404);
    assert_eq!(api_error.message, "Query returned no results");
}

#[tokio::test]
async fn should_handle_serialisation_error() {
    let rejection = serde_json::from_str::<i32>("")
        .map_err(|err| reject::custom(StorageError(DatabaseReadError::Serialisation(err))))
        .unwrap_err();

    let api_error = get_api_error_from_rejection(rejection).await;

    assert_eq!(api_error.code, 500);
    assert_eq!(
        api_error.message,
        "Error deserializing returned data: EOF while parsing a value at line 1 column 0"
    );
}

#[tokio::test]
#[allow(clippy::invalid_regex)]
async fn should_handle_unexpected_error() {
    let rejection = regex::Regex::new("[")
        .map_err(|err| reject::custom(Unexpected(err.into())))
        .unwrap_err();

    let api_error = get_api_error_from_rejection(rejection).await;

    assert_eq!(api_error.code, 500);
    assert_eq!(
        api_error.message,
        "Unexpected error in REST server - please file a bug report!
regex parse error:
    [
    ^
error: unclosed character class"
    );
}
