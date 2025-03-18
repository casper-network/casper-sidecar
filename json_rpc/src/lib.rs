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
//! use casper_json_rpc::{ConfigLimit, Error, Params, RequestHandlersBuilder};
//! use std::{convert::Infallible};
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
//!     let allow_unknown_fields = false;
//!     let route = casper_json_rpc::route(path, max_body_bytes, handlers, allow_unknown_fields);
//!
//!     // Convert it into a `Service` and run it.
//!     let make_svc = hyper::service::make_service_fn(move |_| {
//!         let svc = warp::service(route.clone());
//!         async move { Ok::<_, Infallible>(svc.clone()) }
//!     });
//!
//!     hyper::Server::bind(&([127, 0, 0, 1], 3030).into())
//!         .serve(make_svc)
//!         .await
//!         .unwrap();
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

#![doc(html_root_url = "https://docs.rs/casper-json-rpc/1.1.0")]
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
pub mod rejections;
mod request;
mod request_handlers;
mod response;

use std::{hash::Hash, num::NonZeroU32};

use casper_types::TimeDiff;
use datasize::DataSize;
use governor::Quota;
use http::{header::CONTENT_TYPE, Method};
use serde::Deserialize;
use warp::{filters::BoxedFilter, Filter, Reply};

pub use error::{Error, ErrorCodeT, ReservedErrorCode};
pub use request::Params;
pub use request_handlers::{RequestHandlers, RequestHandlersBuilder};
pub use response::Response;

const JSON_RPC_VERSION: &str = "2.0";

/// Default value for limiter's number of requests.
pub const DEFAULT_LIMIT_REQUESTS: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(10) };
/// Default value for limiter's period of time.
pub const DEFAULT_LIMIT_PERIOD: TimeDiff = TimeDiff::from_seconds(1);

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
        if let Some(quota) = Quota::with_period(self.period.into()) {
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
/// If `allow_unknown_fields` is `false`, requests with unknown fields will cause the server to
/// respond with an error.
///
/// For further details, see the docs for the [`filters`] functions.
pub fn route<P: AsRef<str> + Eq + Hash + Send + Sync + 'static>(
    path: P,
    max_body_bytes: u64,
    handlers: RequestHandlers,
    allow_unknown_fields: bool,
) -> BoxedFilter<(impl Reply,)> {
    filters::base_filter(path, max_body_bytes)
        .and(filters::main_filter(handlers, allow_unknown_fields))
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
/// If `allow_unknown_fields` is `false`, requests with unknown fields will cause the server to
/// respond with an error.
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
    allow_unknown_fields: bool,
    cors_header: &CorsOrigin,
) -> BoxedFilter<(impl Reply,)> {
    filters::base_filter(path, max_body_bytes)
        .and(filters::main_filter(handlers, allow_unknown_fields))
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
