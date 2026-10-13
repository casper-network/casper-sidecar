//! Warp filters which can be combined to provide JSON-RPC endpoints.
//!
//! Generally these lower-level filters will not need to be explicitly called.  Instead,
//! [`casper_json_rpc::route()`](crate::route) should be sufficient.

#[cfg(test)]
mod tests;

use std::{convert::Infallible, hash::Hash};

use bytes::Bytes;
use http::{StatusCode, header::CONTENT_TYPE};
use serde_json::json;
use tracing::{trace, warn};
use warp::{
    Filter, Reply, body,
    filters::{self, BoxedFilter},
    reject::{self, Rejection},
    reply::{self, WithStatus},
};

use crate::{
    Error, JsonRpcOptions, JsonRpcOutput, Response, handle_json_request_bytes,
    rejections::BodyTooLarge, request_handlers::RequestHandlers,
};

/// The HTTP status to use for the response carrying `error`, honoring an override set via
/// [`Error::with_http_status_override`] and falling back to `200 OK` (the JSON-RPC default)
/// otherwise.
fn http_status_of(error: Option<&Error>) -> StatusCode {
    error
        .and_then(Error::http_status_override)
        .and_then(|status| StatusCode::from_u16(status).ok())
        .unwrap_or(StatusCode::OK)
}

const CONTENT_TYPE_VALUE: &str = "application/json";

/// Returns a boxed warp filter which handles the initial setup.
///
/// This includes:
///   * setting the full path
///   * setting the method to POST
///   * ensuring the "content-type" header exists and is set to "application/json"
///   * ensuring the body has at most `max_body_bytes` bytes
pub fn base_filter<P: AsRef<str> + Eq + Hash + Send + Sync + 'static>(
    path: P,
    max_body_bytes: u64,
) -> BoxedFilter<()> {
    warp::path::path(path)
        .and(warp::path::end())
        .and(filters::method::post())
        .and(filters::header::exact_ignore_case(
            CONTENT_TYPE.as_str(),
            CONTENT_TYPE_VALUE,
        ))
        .and(
            body::content_length_limit(max_body_bytes).or_else(move |_rejection| async move {
                Err(reject::custom(BodyTooLarge(max_body_bytes)))
            }),
        )
        .boxed()
}

/// Returns a boxed warp filter which handles parsing a JSON-RPC request from the given HTTP body,
/// executing it using the appropriate handler, and providing a reply.
///
/// Notifications execute and produce an HTTP 204 response with an empty body.
#[must_use]
pub fn main_filter(
    handlers: RequestHandlers,
    options: JsonRpcOptions,
) -> BoxedFilter<(reply::Response,)> {
    body::bytes()
        .then(move |body: Bytes| {
            let mut handlers = handlers.clone();
            async move {
                match handle_json_request_bytes(&body, &mut handlers, &options).await {
                    JsonRpcOutput::NoResponse => {
                        reply::with_status("", StatusCode::NO_CONTENT).into_response()
                    }
                    JsonRpcOutput::Single(response) => {
                        let status = http_status_of(response.error());
                        reply::with_status(reply::json(&response), status).into_response()
                    }
                    JsonRpcOutput::Batch(responses) => {
                        // A batch shares a single outer HTTP response even though it may carry
                        // several JSON-RPC results - use the first response's status override, if
                        // any of them set one.
                        let status = http_status_of(responses.iter().find_map(Response::error));
                        reply::with_status(reply::json(&responses), status).into_response()
                    }
                }
            }
        })
        .boxed()
}

/// Handler for rejections where no JSON-RPC response is sent, but an HTTP response is required.
///
/// The HTTP response body will be a JSON object of the form:
/// ```json
/// { "message": <String> }
/// ```
pub async fn handle_rejection(error: Rejection) -> Result<WithStatus<reply::Json>, Infallible> {
    let code;
    let message;

    if let Some(rejection) = error.find::<BodyTooLarge>() {
        trace!("{rejection:?}");
        message = rejection.to_string();
        code = StatusCode::PAYLOAD_TOO_LARGE;
    } else if error.is_not_found() {
        trace!("{error:?}");
        message = "Path not found".to_string();
        code = StatusCode::NOT_FOUND;
    } else if let Some(rejection) = error.find::<reject::MethodNotAllowed>() {
        trace!("{rejection:?}");
        message = rejection.to_string();
        code = StatusCode::METHOD_NOT_ALLOWED;
    } else if let Some(rejection) = error.find::<reject::InvalidHeader>() {
        trace!("{rejection:?}");
        message = rejection.to_string();
        code = StatusCode::BAD_REQUEST;
    } else if let Some(rejection) = error.find::<reject::MissingHeader>() {
        trace!("{rejection:?}");
        message = rejection.to_string();
        code = StatusCode::UNSUPPORTED_MEDIA_TYPE;
    } else if let Some(rejection) = error.find::<reject::InvalidQuery>() {
        trace!("{rejection:?}");
        message = rejection.to_string();
        code = StatusCode::BAD_REQUEST;
    } else if let Some(rejection) = error.find::<reject::MissingCookie>() {
        trace!("{rejection:?}");
        message = rejection.to_string();
        code = StatusCode::BAD_REQUEST;
    } else if let Some(rejection) = error.find::<reject::LengthRequired>() {
        trace!("{rejection:?}");
        message = rejection.to_string();
        code = StatusCode::LENGTH_REQUIRED;
    } else if let Some(rejection) = error.find::<reject::PayloadTooLarge>() {
        trace!("{rejection:?}");
        message = rejection.to_string();
        code = StatusCode::PAYLOAD_TOO_LARGE;
    } else if let Some(rejection) = error.find::<reject::UnsupportedMediaType>() {
        trace!("{rejection:?}");
        message = rejection.to_string();
        code = StatusCode::UNSUPPORTED_MEDIA_TYPE;
    } else if let Some(rejection) = error.find::<filters::cors::CorsForbidden>() {
        trace!("{rejection:?}");
        message = rejection.to_string();
        code = StatusCode::FORBIDDEN;
    } else {
        // We should handle all rejection types before this.
        warn!(?error, "unhandled warp rejection in json-rpc server");
        message = format!("Internal server error: unhandled rejection: {error:?}");
        code = StatusCode::INTERNAL_SERVER_ERROR;
    }

    Ok(reply::with_status(
        reply::json(&json!({ "message": message })),
        code,
    ))
}
