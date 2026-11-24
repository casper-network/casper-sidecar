//! # casper-json-rpc
//!
//! A library suitable for use as the framework for a JSON-RPC server.
//!
//! # Usage
//!
//! Normally usage will involve two steps:
//!   * construct a set of request handlers using a [`RequestHandlersBuilder`]
//!   * call [`casper_json_rpc::route`](route) to construct a boxed warp filter ready to be passed
//!     to [`warp::service`](https://docs.rs/warp/latest/warp/fn.service.html) for example
//!
//! # Example
//!
//! ```no_run
//! use casper_json_rpc::{ConfigLimit, Error, JsonRpcOptions, Params, RequestHandlersBuilder};
//!
//! # #[allow(unused)]
//! async fn get(params: Option<Params>) -> Result<String, Error> {
//!     // * parse params or return `ReservedErrorCode::InvalidParams` error
//!     // * handle request and return result
//!     Ok("got it".to_string())
//! }
//!
//! # #[allow(unused)]
//! async fn put(params: Option<Params>, other_input: &str) -> Result<String, Error> {
//!     Ok(other_input.to_string())
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     // Register handlers for methods "get" and "put".
//!     let mut handlers = RequestHandlersBuilder::new();
//!     let limit = ConfigLimit::default();
//!     handlers.register_handler("get", get, &limit);
//!     let put_handler = move |params| async move { put(params, "other input").await };
//!     handlers.register_handler("put", put_handler, &limit);
//!     let handlers = handlers.build();
//!
//!     // Get the new route.
//!     let path = "rpc";
//!     let max_body_bytes = 1024;
//!     let options = JsonRpcOptions::default();
//!     let route = casper_json_rpc::route(path, max_body_bytes, handlers, options);
//!
//!     warp::serve(route).run(([127, 0, 0, 1], 3030)).await;
//! }
//! ```
//!
//! # Errors
//!
//! To return a JSON-RPC response indicating an error, use [`Error::new`].  Most error conditions
//! which require returning a reserved error are already handled in the provided warp filters.  The
//! only exception is [`ReservedErrorCode::InvalidParams`] which should be returned by any RPC
//! handler which deems the provided `params: Option<Params>` to be invalid for any reason.
//!
//! Generally a set of custom error codes should be provided.  These should all implement
//! [`ErrorCodeT`].

#![doc(html_root_url = "https://docs.rs/casper-json-rpc/3.0.0")]
#![doc(
    html_favicon_url = "https://raw.githubusercontent.com/casper-network/casper-node/blob/dev/images/Casper_Logo_Favicon_48.png",
    html_logo_url = "https://raw.githubusercontent.com/casper-network/casper-node/blob/dev/images/Casper_Logo_Favicon.png",
    test(attr(deny(warnings)))
)]
#![warn(
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unused_qualifications
)]

mod error;
pub mod filters;
mod notification;
pub mod rejections;
mod request;
mod request_handlers;
mod response;

use std::{
    hash::Hash,
    num::{NonZeroU32, NonZeroU64},
    time::Duration,
};

use casper_types::TimeDiff;
use datasize::DataSize;
use governor::Quota;
use http::{Method, header::CONTENT_TYPE};
use metrics::rpc::{
    inc_batch_count_limit_rejection, inc_batch_response_limit_truncation, observe_batch_length,
    observe_batch_response_bytes,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use warp::{Filter, Reply, filters::BoxedFilter};

pub use error::{Error, ErrorCodeT, ReservedErrorCode, RpcErrorCode};
pub use notification::Notification;
pub use request::Params;
pub use request_handlers::{
    MethodLimiter, RequestDispatcher, RequestHandlers, RequestHandlersBuilder,
};
pub use response::Response;

const JSON_RPC_VERSION: &str = "2.0";

/// Default value for limiter's number of requests.
pub const DEFAULT_LIMIT_REQUESTS: NonZeroU32 = NonZeroU32::new(10).unwrap();
/// Default value for limiter's period of time.
pub const DEFAULT_LIMIT_PERIOD: TimeDiff = TimeDiff::from_seconds(1);
/// Default maximum number of entries accepted in one JSON-RPC batch.
pub const DEFAULT_MAX_BATCH_ITEMS: NonZeroU32 = NonZeroU32::new(100).unwrap();
/// Default soft maximum size of a serialized JSON-RPC batch response.
pub const DEFAULT_MAX_BATCH_RESPONSE_BYTES: NonZeroU64 = NonZeroU64::new(25_000_000).unwrap();

/// Options controlling JSON-RPC request validation and batching.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JsonRpcOptions {
    /// Whether unknown request-object fields are accepted.
    pub allow_unknown_fields: bool,
    /// Maximum number of entries accepted in a batch.
    pub max_batch_items: NonZeroU32,
    /// Soft maximum serialized batch response size.
    pub max_batch_response_bytes: NonZeroU64,
}

impl Default for JsonRpcOptions {
    fn default() -> Self {
        Self {
            allow_unknown_fields: false,
            max_batch_items: DEFAULT_MAX_BATCH_ITEMS,
            max_batch_response_bytes: DEFAULT_MAX_BATCH_RESPONSE_BYTES,
        }
    }
}

/// Transport-neutral result of processing one JSON-RPC payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JsonRpcOutput {
    /// A notification-only payload, for which no response is allowed.
    NoResponse,
    /// A response to a single request, parse error, invalid value, or empty batch.
    Single(Response),
    /// Responses to a batch. This vector is guaranteed to be non-empty; notification responses
    /// are omitted.
    Batch(Vec<Response>),
}

impl JsonRpcOutput {
    /// Converts this output to a JSON value, or returns `None` when no response is allowed.
    #[must_use]
    pub fn into_value(self) -> Option<Value> {
        match self {
            JsonRpcOutput::NoResponse => None,
            JsonRpcOutput::Single(response) => {
                Some(serde_json::to_value(response).expect("JSON-RPC response should serialize"))
            }
            JsonRpcOutput::Batch(responses) => {
                Some(serde_json::to_value(responses).expect("JSON-RPC responses should serialize"))
            }
        }
    }
}

/// Handles a raw JSON-RPC request body without requiring an HTTP transport.
///
/// This is intended for transports such as WebSocket that need to reuse the same request
/// validation, batching, and dispatch semantics as the HTTP JSON-RPC route.
#[must_use]
pub async fn handle_json_request_bytes<D: RequestDispatcher + ?Sized>(
    body: &[u8],
    dispatcher: &mut D,
    options: &JsonRpcOptions,
) -> JsonRpcOutput {
    let value = match serde_json::from_slice::<Value>(body) {
        Ok(value) => value,
        Err(error) => {
            return JsonRpcOutput::Single(Response::new_failure(
                Value::Null,
                Error::new(ReservedErrorCode::ParseError, error.to_string()),
            ));
        }
    };

    match value {
        Value::Object(request) => {
            match request::Request::new(request, options.allow_unknown_fields) {
                Ok(request) => dispatch_request(request, body.len(), dispatcher)
                    .await
                    .map_or(JsonRpcOutput::NoResponse, JsonRpcOutput::Single),
                Err(error) => JsonRpcOutput::Single(Response::new_failure(error.id, error.error)),
            }
        }
        Value::Array(requests) => handle_batch(requests, dispatcher, options).await,
        _ => JsonRpcOutput::Single(invalid_request_response(
            "top-level JSON-RPC payload must be an object or array",
        )),
    }
}

enum BatchEntry {
    Request(request::Request, usize),
    Invalid(Response),
}

#[derive(Serialize)]
struct BatchItemLimitErrorData {
    message: &'static str,
    actual: usize,
    maximum: u32,
}

#[derive(Serialize)]
struct BatchResponseLimitErrorData {
    message: &'static str,
    maximum: u64,
}

async fn handle_batch<D: RequestDispatcher + ?Sized>(
    requests: Vec<Value>,
    dispatcher: &mut D,
    options: &JsonRpcOptions,
) -> JsonRpcOutput {
    observe_batch_length(requests.len());
    if requests.is_empty() {
        let response =
            invalid_request_response("a JSON-RPC batch must contain at least one request");
        observe_batch_response_bytes(serialized_response_len(&response));
        return JsonRpcOutput::Single(response);
    }
    if requests.len() > options.max_batch_items.get() as usize {
        inc_batch_count_limit_rejection();
        let responses = vec![invalid_request_response(BatchItemLimitErrorData {
            message: "JSON-RPC batch exceeds maximum item count",
            actual: requests.len(),
            maximum: options.max_batch_items.get(),
        })];
        observe_batch_response_bytes(serialized_responses_len(&responses));
        return JsonRpcOutput::Batch(responses);
    }

    let entries = requests
        .into_iter()
        .map(|value| {
            let request_size = serde_json::to_vec(&value).map_or(0, |bytes| bytes.len());
            match value {
                Value::Object(request) => {
                    match request::Request::new(request, options.allow_unknown_fields) {
                        Ok(request) => BatchEntry::Request(request, request_size),
                        Err(error) => {
                            BatchEntry::Invalid(Response::new_failure(error.id, error.error))
                        }
                    }
                }
                _ => BatchEntry::Invalid(invalid_request_response(
                    "a JSON-RPC batch entry must be an object",
                )),
            }
        })
        .collect::<Vec<_>>();

    let mut responses = Vec::new();
    let mut response_bytes = 2u64;
    let mut response_limit_reached = false;
    let mut response_limit_truncation_recorded = false;

    for entry in entries {
        match entry {
            BatchEntry::Invalid(response) => {
                append_response(&mut responses, &mut response_bytes, response);
            }
            BatchEntry::Request(request, request_size) if request.id.is_none() => {
                let _ = dispatch_request(request, request_size, dispatcher).await;
            }
            BatchEntry::Request(request, _request_size)
                if response_limit_reached
                    || response_bytes >= options.max_batch_response_bytes.get() =>
            {
                response_limit_reached = true;
                if !response_limit_truncation_recorded {
                    inc_batch_response_limit_truncation();
                    response_limit_truncation_recorded = true;
                }
                let id = request
                    .id
                    .expect("non-notification request should have an id");
                append_response(
                    &mut responses,
                    &mut response_bytes,
                    response_limit_error(id, options.max_batch_response_bytes.get()),
                );
            }
            BatchEntry::Request(request, request_size) => {
                let id = request
                    .id
                    .clone()
                    .expect("non-notification request should have an id");
                let separator = u64::from(!responses.is_empty());
                if response_bytes
                    .saturating_add(separator)
                    .saturating_add(minimum_response_len(&id))
                    > options.max_batch_response_bytes.get()
                {
                    response_limit_reached = true;
                    if !response_limit_truncation_recorded {
                        inc_batch_response_limit_truncation();
                        response_limit_truncation_recorded = true;
                    }
                    append_response(
                        &mut responses,
                        &mut response_bytes,
                        response_limit_error(id, options.max_batch_response_bytes.get()),
                    );
                    continue;
                }
                let response = dispatch_request(request, request_size, dispatcher)
                    .await
                    .expect("non-notification request should produce a response");
                let serialized_len = serialized_response_len(&response);
                if response_bytes
                    .saturating_add(separator)
                    .saturating_add(serialized_len)
                    > options.max_batch_response_bytes.get()
                {
                    response_limit_reached = true;
                    // The call has already executed, so preserve its real response. Returning a
                    // throttling error here could cause clients to retry a successful side effect.
                }
                append_response(&mut responses, &mut response_bytes, response);
            }
        }
    }

    if responses.is_empty() {
        observe_batch_response_bytes(0);
        JsonRpcOutput::NoResponse
    } else {
        observe_batch_response_bytes(response_bytes);
        JsonRpcOutput::Batch(responses)
    }
}

async fn dispatch_request<D: RequestDispatcher + ?Sized>(
    request: request::Request,
    request_size: usize,
    dispatcher: &mut D,
) -> Option<Response> {
    let request::Request { id, method, params } = request;
    let result = dispatcher.dispatch(&method, params, request_size).await;
    id.map(|id| match result {
        Ok(value) => Response::new_success(id, value),
        Err(error) => Response::new_failure(id, error),
    })
}

fn invalid_request_response(additional_info: impl Serialize) -> Response {
    Response::new_failure(
        Value::Null,
        Error::new(ReservedErrorCode::InvalidRequest, additional_info),
    )
}

fn response_limit_error(id: Value, maximum: u64) -> Response {
    Response::new_failure(
        id,
        Error::new(
            RpcErrorCode::RequestThrottled,
            BatchResponseLimitErrorData {
                message: "JSON-RPC batch response size limit exceeded",
                maximum,
            },
        ),
    )
}

fn serialized_response_len(response: &Response) -> u64 {
    serde_json::to_vec(response).map_or(u64::MAX, |bytes| bytes.len() as u64)
}

fn minimum_response_len(id: &Value) -> u64 {
    serialized_response_len(&Response::new_success(id.clone(), json!(0)))
}

fn serialized_responses_len(responses: &[Response]) -> u64 {
    serde_json::to_vec(responses).map_or(u64::MAX, |bytes| bytes.len() as u64)
}

fn append_response(responses: &mut Vec<Response>, response_bytes: &mut u64, response: Response) {
    if !responses.is_empty() {
        *response_bytes = response_bytes.saturating_add(1);
    }
    *response_bytes = response_bytes.saturating_add(serialized_response_len(&response));
    responses.push(response);
}

/// Specifies the CORS origin
pub enum CorsOrigin {
    /// Any (*) origin is allowed.
    Any,
    /// Only the specified origin is allowed.
    Specified(String),
}

/// Helper function for `DataSize` derive.
#[must_use]
pub fn nonzero_u32(value: &NonZeroU32) -> usize {
    value.get().estimate_heap_size()
}

/// Helper function for `DataSize` derive.
#[must_use]
pub fn nonzero_u64(value: &NonZeroU64) -> usize {
    value.get().estimate_heap_size()
}

/// Specifies connection rate limiter parameters for a method (HTTP path).
#[derive(Clone, DataSize, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigLimit {
    /// Maximum number of request that the rate limiter will allow for a period of time (below).
    #[data_size(with = nonzero_u32)]
    pub requests: NonZeroU32,
    /// Rate limiter's time period.
    pub period: TimeDiff,
}

impl ConfigLimit {
    /// Return connection limit as `Quota`.
    #[must_use]
    pub fn quota(&self) -> Quota {
        // Governor keeps N slots. Each slot expires within a given period.
        // To keep the desired rate, the period has to be divided by N.
        let period: Duration = self.period.into();
        if let Some(quota) = Quota::with_period(period / self.requests.get()) {
            quota.allow_burst(self.requests)
        } else {
            Quota::per_second(self.requests)
        }
    }
}

impl Default for ConfigLimit {
    fn default() -> Self {
        Self {
            requests: DEFAULT_LIMIT_REQUESTS,
            period: DEFAULT_LIMIT_PERIOD,
        }
    }
}

/// Constructs a set of warp filters suitable for use in a JSON-RPC server.
///
/// `path` specifies the exact HTTP path for JSON-RPC requests, e.g. "rpc" will match requests on
/// exactly "/rpc", and not "/rpc/other".
///
/// `max_body_bytes` sets an upper limit for the number of bytes in the HTTP request body.  For
/// further details, see
/// [`warp::filters::body::content_length_limit`](https://docs.rs/warp/latest/warp/filters/body/fn.content_length_limit.html).
///
/// `handlers` is the map of functions to which incoming requests will be dispatched.  These are
/// keyed by the JSON-RPC request's "method".
///
/// `options` controls validation and batch resource limits.
///
/// For further details, see the docs for the [`filters`] functions.
pub fn route<P: AsRef<str> + Eq + Hash + Send + Sync + 'static>(
    path: P,
    max_body_bytes: u64,
    handlers: RequestHandlers,
    options: JsonRpcOptions,
) -> BoxedFilter<(impl Reply,)> {
    filters::base_filter(path, max_body_bytes)
        .and(filters::main_filter(handlers, options))
        .recover(filters::handle_rejection)
        .boxed()
}

/// Constructs a set of warp filters suitable for use in a JSON-RPC server.
///
/// `path` specifies the exact HTTP path for JSON-RPC requests, e.g. "rpc" will match requests on
/// exactly "/rpc", and not "/rpc/other".
///
/// `max_body_bytes` sets an upper limit for the number of bytes in the HTTP request body.  For
/// further details, see
/// [`warp::filters::body::content_length_limit`](https://docs.rs/warp/latest/warp/filters/body/fn.content_length_limit.html).
///
/// `handlers` is the map of functions to which incoming requests will be dispatched.  These are
/// keyed by the JSON-RPC request's "method".
///
/// `options` controls validation and batch resource limits.
///
/// Note that this is a convenience function combining the lower-level functions in [`filters`]
/// along with [a warp CORS filter](https://docs.rs/warp/latest/warp/filters/cors/index.html) which
///   * allows any origin or specified origin
///   * allows "content-type" as a header
///   * allows the method "POST"
///
/// For further details, see the docs for the [`filters`] functions.
pub fn route_with_cors<P: AsRef<str> + Eq + Hash + Send + Sync + 'static>(
    path: P,
    max_body_bytes: u64,
    handlers: RequestHandlers,
    options: JsonRpcOptions,
    cors_header: CorsOrigin,
) -> BoxedFilter<(impl Reply,)> {
    filters::base_filter(path, max_body_bytes)
        .and(filters::main_filter(handlers, options))
        .recover(filters::handle_rejection)
        .with(match cors_header {
            CorsOrigin::Any => warp::cors()
                .allow_any_origin()
                .allow_header(CONTENT_TYPE)
                .allow_method(Method::POST),
            CorsOrigin::Specified(origin) => warp::cors()
                .allow_origin(origin.as_str())
                .allow_header(CONTENT_TYPE)
                .allow_method(Method::POST),
        })
        .boxed()
}

#[cfg(test)]
mod processor_tests {
    use std::num::{NonZeroU32, NonZeroU64};

    use http::StatusCode;
    use serde_json::{Value, json};

    use super::*;

    #[derive(Default)]
    struct RecordingDispatcher {
        calls: Vec<(String, Option<Params>, usize)>,
    }

    impl RequestDispatcher for RecordingDispatcher {
        async fn dispatch(
            &mut self,
            method: &str,
            params: Option<Params>,
            request_size: usize,
        ) -> Result<Value, Error> {
            self.calls
                .push((method.to_string(), params.clone(), request_size));
            match method {
                "sum" => {
                    let sum = params
                        .and_then(|params| params.as_array().cloned())
                        .unwrap_or_default()
                        .into_iter()
                        .filter_map(|value| value.as_i64())
                        .sum::<i64>();
                    Ok(json!(sum))
                }
                "subtract" => {
                    let values = params
                        .and_then(|params| params.as_array().cloned())
                        .unwrap_or_default();
                    Ok(json!(
                        values[0].as_i64().unwrap() - values[1].as_i64().unwrap()
                    ))
                }
                "large" => Ok(json!("x".repeat(1_000))),
                "fail" => Err(Error::new(ReservedErrorCode::InvalidParams, "failed")),
                _ => Ok(json!(method)),
            }
        }
    }

    async fn process(body: &Value, dispatcher: &mut impl RequestDispatcher) -> JsonRpcOutput {
        handle_json_request_bytes(
            &serde_json::to_vec(body).unwrap(),
            dispatcher,
            &JsonRpcOptions::default(),
        )
        .await
    }

    fn as_value(output: JsonRpcOutput) -> Value {
        output.into_value().expect("expected JSON-RPC response")
    }

    #[tokio::test]
    async fn runs_official_style_mixed_batch_in_response_order() {
        let mut dispatcher = RecordingDispatcher::default();
        let output = process(
            &json!([
                {"jsonrpc":"2.0","method":"sum","params":[1,2,4],"id":"1"},
                {"jsonrpc":"2.0","method":"notify","params":[7]},
                {"jsonrpc":"2.0","method":"subtract","params":[42,23],"id":2},
                {"foo":"boo"},
                {"jsonrpc":"2.0","method":"fail","id":3}
            ]),
            &mut dispatcher,
        )
        .await;

        let response = as_value(output);
        assert_eq!(response[0], json!({"jsonrpc":"2.0","id":"1","result":7}));
        assert_eq!(response[1], json!({"jsonrpc":"2.0","id":2,"result":19}));
        assert_eq!(response[2]["id"], Value::Null);
        assert_eq!(response[2]["error"]["code"], -32600);
        assert_eq!(response[3]["id"], 3);
        assert_eq!(response[3]["error"]["code"], -32602);
        assert_eq!(
            dispatcher
                .calls
                .iter()
                .map(|call| call.0.as_str())
                .collect::<Vec<_>>(),
            ["sum", "notify", "subtract", "fail"]
        );
        let element_size =
            serde_json::to_vec(&json!({"jsonrpc":"2.0","method":"sum","params":[1,2,4],"id":"1"}))
                .unwrap()
                .len();
        assert_eq!(dispatcher.calls[0].2, element_size);
    }

    #[tokio::test]
    async fn handles_top_level_and_batch_shapes() {
        let mut dispatcher = RecordingDispatcher::default();
        let malformed =
            handle_json_request_bytes(b"{", &mut dispatcher, &JsonRpcOptions::default()).await;
        assert_eq!(as_value(malformed)["error"]["code"], -32700);

        for invalid in [json!(null), json!(true), json!(7), json!("request")] {
            let response = as_value(process(&invalid, &mut dispatcher).await);
            assert_eq!(response["id"], Value::Null);
            assert_eq!(response["error"]["code"], -32600);
        }

        let empty = as_value(process(&json!([]), &mut dispatcher).await);
        assert!(empty.is_object());
        assert_eq!(empty["error"]["code"], -32600);

        let singleton = as_value(
            process(
                &json!([{"jsonrpc":"2.0","method":"sum","params":[],"id":1}]),
                &mut dispatcher,
            )
            .await,
        );
        assert!(singleton.is_array());
        assert_eq!(singleton.as_array().unwrap().len(), 1);

        let invalid = as_value(process(&json!([[1], null, true]), &mut dispatcher).await);
        assert_eq!(invalid.as_array().unwrap().len(), 3);
        assert!(
            invalid
                .as_array()
                .unwrap()
                .iter()
                .all(|response| response["error"]["code"] == -32600)
        );
    }

    #[tokio::test]
    async fn distinguishes_notifications_null_ids_and_invalid_idless_objects() {
        let mut dispatcher = RecordingDispatcher::default();
        let output = process(
            &json!([
                {"jsonrpc":"2.0","method":"notify"},
                {"jsonrpc":"2.0","method":"sum","params":[],"id":null},
                {"jsonrpc":"2.0","params":[]},
                {"jsonrpc":"2.0","method":"sum","params":null}
            ]),
            &mut dispatcher,
        )
        .await;
        let response = as_value(output);
        assert_eq!(response.as_array().unwrap().len(), 3);
        assert_eq!(response[0]["id"], Value::Null);
        assert_eq!(response[0]["result"], 0);
        assert_eq!(response[1]["error"]["code"], -32600);
        assert_eq!(response[2]["error"]["code"], -32600);
        assert_eq!(dispatcher.calls.len(), 2);

        let output = process(
            &json!([
                {"jsonrpc":"2.0","method":"notify"},
                {"jsonrpc":"2.0","method":"notify","params":[]}
            ]),
            &mut dispatcher,
        )
        .await;
        assert_eq!(output, JsonRpcOutput::NoResponse);
        assert_eq!(dispatcher.calls.len(), 4);
    }

    #[tokio::test]
    async fn accepts_json_number_and_duplicate_ids_but_rejects_other_id_types() {
        let mut dispatcher = RecordingDispatcher::default();
        let response = as_value(
            process(
                &json!([
                    {"jsonrpc":"2.0","method":"sum","params":[],"id":"same"},
                    {"jsonrpc":"2.0","method":"sum","params":[],"id":12},
                    {"jsonrpc":"2.0","method":"sum","params":[],"id":1.25},
                    {"jsonrpc":"2.0","method":"sum","params":[],"id":"same"},
                    {"jsonrpc":"2.0","method":"sum","params":[],"id":true},
                    {"jsonrpc":"2.0","method":"sum","params":[],"id":{}},
                    {"jsonrpc":"2.0","method":"sum","params":[],"id":[]}
                ]),
                &mut dispatcher,
            )
            .await,
        );
        let ids = response
            .as_array()
            .unwrap()
            .iter()
            .map(|response| response["id"].clone())
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            [
                json!("same"),
                json!(12),
                json!(1.25),
                json!("same"),
                Value::Null,
                Value::Null,
                Value::Null
            ]
        );
        assert_eq!(dispatcher.calls.len(), 4);
    }

    #[tokio::test]
    async fn echoes_high_precision_fractional_id_exactly() {
        const ID: &[u8] = b"0.123456789012345678901234567890";
        let body = [
            br#"{"jsonrpc":"2.0","method":"sum","params":[],"id":"#.as_slice(),
            ID,
            b"}",
        ]
        .concat();
        let mut dispatcher = RecordingDispatcher::default();

        let output =
            handle_json_request_bytes(&body, &mut dispatcher, &JsonRpcOptions::default()).await;
        let JsonRpcOutput::Single(response) = output else {
            panic!("expected a single JSON-RPC response");
        };

        assert_eq!(serde_json::to_vec(response.id()).unwrap(), ID);
    }

    #[tokio::test]
    async fn enforces_batch_count_without_partial_execution() {
        let mut dispatcher = RecordingDispatcher::default();
        let hundred = Value::Array(
            (0..100)
                .map(|_| json!({"jsonrpc":"2.0","method":"notify"}))
                .collect(),
        );
        assert_eq!(
            process(&hundred, &mut dispatcher).await,
            JsonRpcOutput::NoResponse
        );
        assert_eq!(dispatcher.calls.len(), 100);

        dispatcher.calls.clear();
        let hundred_one = Value::Array(
            (0..101)
                .map(|_| json!({"jsonrpc":"2.0","method":"notify"}))
                .collect(),
        );
        let response = as_value(process(&hundred_one, &mut dispatcher).await);
        assert_eq!(response.as_array().unwrap().len(), 1);
        assert_eq!(response[0]["error"]["code"], -32600);
        assert_eq!(response[0]["error"]["data"]["actual"], 101);
        assert!(dispatcher.calls.is_empty());
    }

    #[tokio::test]
    async fn replaces_response_cap_overflow_and_later_calls_but_runs_notifications() {
        let mut dispatcher = RecordingDispatcher::default();
        let options = JsonRpcOptions {
            max_batch_response_bytes: NonZeroU64::new(70).unwrap(),
            ..JsonRpcOptions::default()
        };
        let body = json!([
            {"jsonrpc":"2.0","method":"sum","params":[],"id":1},
            {"jsonrpc":"2.0","method":"large","id":"large"},
            {"jsonrpc":"2.0","method":"notify"},
            {"jsonrpc":"2.0","method":"sum","params":[],"id":3},
            false
        ]);
        let output = handle_json_request_bytes(
            &serde_json::to_vec(&body).unwrap(),
            &mut dispatcher,
            &options,
        )
        .await;
        let response = as_value(output);
        assert_eq!(response[0]["id"], 1);
        assert_eq!(response[1]["id"], "large");
        assert_eq!(response[1]["error"]["code"], 429);
        assert_eq!(response[2]["id"], 3);
        assert_eq!(response[2]["error"]["code"], 429);
        assert_eq!(response[3]["id"], Value::Null);
        assert_eq!(response[3]["error"]["code"], -32600);
        assert_eq!(
            dispatcher
                .calls
                .iter()
                .map(|call| call.0.as_str())
                .collect::<Vec<_>>(),
            ["sum", "notify"]
        );
    }

    #[tokio::test]
    async fn preserves_an_executed_overflow_response_and_skips_later_calls() {
        let mut dispatcher = RecordingDispatcher::default();
        let id = json!("large");
        let options = JsonRpcOptions {
            max_batch_response_bytes: NonZeroU64::new(2 + minimum_response_len(&id)).unwrap(),
            ..JsonRpcOptions::default()
        };
        let body = json!([
            {"jsonrpc":"2.0","method":"large","id":id},
            {"jsonrpc":"2.0","method":"notify"},
            {"jsonrpc":"2.0","method":"sum","params":[],"id":2},
            false
        ]);

        let output = handle_json_request_bytes(
            &serde_json::to_vec(&body).unwrap(),
            &mut dispatcher,
            &options,
        )
        .await;
        let response = as_value(output);

        assert_eq!(response[0]["id"], "large");
        assert_eq!(response[0]["result"], json!("x".repeat(1_000)));
        assert_eq!(response[1]["id"], 2);
        assert_eq!(response[1]["error"]["code"], 429);
        assert_eq!(response[2]["id"], Value::Null);
        assert_eq!(response[2]["error"]["code"], -32600);
        assert_eq!(
            dispatcher
                .calls
                .iter()
                .map(|call| call.0.as_str())
                .collect::<Vec<_>>(),
            ["large", "notify"]
        );
    }

    #[tokio::test]
    async fn applies_per_method_limit_to_each_batch_entry() {
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut builder = RequestHandlersBuilder::new();
        let handler_calls = calls.clone();
        builder.register_handler(
            "limited",
            move |_| {
                let handler_calls = handler_calls.clone();
                async move {
                    handler_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Ok(json!(true))
                }
            },
            &ConfigLimit {
                requests: NonZeroU32::new(1).unwrap(),
                period: TimeDiff::from_seconds(60),
            },
        );
        let mut handlers = builder.build();
        let response = as_value(
            process(
                &json!([
                    {"jsonrpc":"2.0","method":"limited","id":1},
                    {"jsonrpc":"2.0","method":"limited","id":2}
                ]),
                &mut handlers,
            )
            .await,
        );
        assert_eq!(response[0]["result"], true);
        assert_eq!(response[1]["error"]["code"], 429);
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn http_batch_response_supports_cors() {
        let mut builder = RequestHandlersBuilder::new();
        builder.register_handler(
            "echo",
            |params| async move { Ok(params.map(Value::from).unwrap_or(Value::Null)) },
            &ConfigLimit::default(),
        );
        let routes = route_with_cors(
            "rpc",
            1_024,
            builder.build(),
            JsonRpcOptions::default(),
            CorsOrigin::Specified("https://example.com".to_string()),
        );
        let response = warp::test::request()
            .path("/rpc")
            .method("POST")
            .header("content-type", "application/json")
            .header("origin", "https://example.com")
            .body(
                json!([
                    {"jsonrpc":"2.0","method":"echo","params":[1],"id":1},
                    {"jsonrpc":"2.0","method":"echo","params":[2],"id":2}
                ])
                .to_string(),
            )
            .reply(&routes)
            .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["access-control-allow-origin"],
            "https://example.com"
        );
        assert!(!response.body().is_empty());
    }
}
