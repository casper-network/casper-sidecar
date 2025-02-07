use std::{
    future::Future,
    hash::Hash,
    mem,
    net::{IpAddr, SocketAddr},
    num::NonZeroU32,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use forwarded_header_value::{ForwardedHeaderValue, Identifier};
use governor::{DefaultKeyedRateLimiter, Quota};
use http::{header::FORWARDED, request::Request, HeaderMap, Response, StatusCode};
use hyper::server::conn::AddrStream;
use pin_project::pin_project;
use thiserror::Error;
use tower::{Layer, Service};

type AddrRateLimiter = DefaultKeyedRateLimiter<SocketAddr>;

pub struct Governor<S> {
    inner: S,
    limiter: Arc<AddrRateLimiter>,
}

impl<S> Governor<S> {
    #[must_use]
    pub fn new(inner: S, period: Duration, burst_size: u32) -> Self {
        let limiter = Arc::new(AddrRateLimiter::keyed(
            Quota::with_period(period)
                .unwrap()
                .allow_burst(NonZeroU32::new(burst_size).unwrap()),
        ));
        Self { inner, limiter }
    }
}

/// The error type returned by tower-governor.
#[derive(Clone, Debug, Error)]
pub enum GovernorError {
    #[error("Too Many Requests! Wait for {wait_time}s")]
    TooManyRequests {
        wait_time: u64,
        headers: Option<HeaderMap>,
    },
    #[error("Unable to extract key!")]
    UnableToExtractKey,
    #[error("Other Error")]
    /// Used for custom key extractors to return their own errors
    Other {
        code: StatusCode,
        msg: Option<String>,
        headers: Option<HeaderMap>,
    },
}

impl GovernorError {
    /// Convert self into a "default response", as if no error handler was set using
    /// [`GovernorConfigBuilder::error_handler`].
    pub fn as_response<ResB>(&mut self) -> Response<ResB>
    where
        ResB: From<String>,
    {
        match mem::replace(self, Self::UnableToExtractKey) {
            GovernorError::TooManyRequests { wait_time, headers } => {
                let response = Response::new(format!("Too Many Requests! Wait for {wait_time}s"));
                let (mut parts, body) = response.into_parts();
                parts.status = StatusCode::TOO_MANY_REQUESTS;
                if let Some(headers) = headers {
                    parts.headers = headers;
                }
                Response::from_parts(parts, ResB::from(body))
            }
            GovernorError::UnableToExtractKey => {
                let response = Response::new("Unable To Extract Key!".into());
                let (mut parts, body) = response.into_parts();
                parts.status = StatusCode::INTERNAL_SERVER_ERROR;

                Response::from_parts(parts, ResB::from(body))
            }
            GovernorError::Other { msg, code, headers } => {
                let response = Response::new("Other Error!".into());
                let (mut parts, mut body) = response.into_parts();
                parts.status = code;
                if let Some(headers) = headers {
                    parts.headers = headers;
                }
                if let Some(msg) = msg {
                    body = msg;
                }

                Response::from_parts(parts, ResB::from(body))
            }
        }
    }
}

// Utility functions for the SmartIpExtractor
// Shamelessly snatched from the axum-client-ip crate here:
// https://crates.io/crates/axum-client-ip

const X_REAL_IP: &str = "x-real-ip";
const X_FORWARDED_FOR: &str = "x-forwarded-for";

/// Tries to parse the `x-forwarded-for` header
fn maybe_x_forwarded_for(headers: &HeaderMap) -> Option<IpAddr> {
    headers
        .get(X_FORWARDED_FOR)
        .and_then(|hv| hv.to_str().ok())
        .and_then(|s| s.split(',').find_map(|s| s.trim().parse::<IpAddr>().ok()))
}

/// Tries to parse the `x-real-ip` header
fn maybe_x_real_ip(headers: &HeaderMap) -> Option<IpAddr> {
    headers
        .get(X_REAL_IP)
        .and_then(|hv| hv.to_str().ok())
        .and_then(|s| s.parse::<IpAddr>().ok())
}

/// Tries to parse `forwarded` headers
fn maybe_forwarded(headers: &HeaderMap) -> Option<IpAddr> {
    headers.get_all(FORWARDED).iter().find_map(|hv| {
        hv.to_str()
            .ok()
            .and_then(|s| ForwardedHeaderValue::from_forwarded(s).ok())
            .and_then(|f| {
                f.iter()
                    .filter_map(|fs| fs.forwarded_for.as_ref())
                    .find_map(|ff| match ff {
                        Identifier::SocketAddr(a) => Some(a.ip()),
                        Identifier::IpAddr(ip) => Some(*ip),
                        _ => None,
                    })
            })
    })
}

fn maybe_connect_info<T>(req: &Request<T>) -> Option<IpAddr> {
    req.extensions().get::<SocketAddr>().map(|addr| addr.ip())
}

pub trait KeyExtractor: Clone {
    type Key: Clone + Eq + Hash;

    fn extract<T>(req: &Request<T>) -> Result<Self::Key, GovernorError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SmartIpKeyExtractor;
impl SmartIpKeyExtractor {
    fn extract<T>(req: &Request<T>) -> Result<IpAddr, GovernorError> {
        let headers = req.headers();

        maybe_x_forwarded_for(headers)
            .or_else(|| maybe_x_real_ip(headers))
            .or_else(|| maybe_forwarded(headers))
            .or_else(|| maybe_connect_info(req))
            .ok_or(GovernorError::UnableToExtractKey)
    }
}

#[pin_project]
pub struct ResponseFuture<F> {
    #[pin]
    response_future: F,
}

impl<F, Response, E> Future for ResponseFuture<F>
where
    // F: Future<Output = Result<Response<Body>, Error>>,
    F: Future<Output = Result<Response, E>>,
{
    type Output = Result<Response, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.response_future.poll(cx) {
            Poll::Ready(result) => {
                return Poll::Ready(result);
            }
            Poll::Pending => {}
        }

        Poll::Pending
    }
}

impl<'a, S> Service<&'a AddrStream> for Governor<S>
where
    S: Service<&'a AddrStream>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: &'a AddrStream) -> Self::Future {
        let remote_addr = request.remote_addr();
        eprintln!("ADAM {remote_addr}");
        let response_future = self.inner.call(request);
        response_future
        // if let Some(configured_methods) = &self.methods {
        //     if !configured_methods.contains(req.method()) {
        //         // The request method is not configured, we're ignoring this one.
        //         let future = self.inner.call(req);
        //         return ResponseFuture {
        //             inner: Kind::Passthrough { future },
        //         };
        //     }
        // }
        // Use the provided key extractor to extract the rate limiting key from the request.
        // match SmartIpKeyExtractor::extract(&req) {
        //     // Extraction worked, let's check if rate limiting is needed.
        //     Ok(key) => match self.limiter.check_key(&key) {
        //         Ok(_) => {
        //             let future = self.inner.call(req);
        //             ResponseFuture {
        //                 inner: Kind::Passthrough { future },
        //             }
        //         }

        //         Err(negative) => {
        //             let wait_time = negative
        //                 .wait_time_from(DefaultClock::default().now())
        //                 .as_secs();

        //             let mut headers = HeaderMap::new();
        //             headers.insert("x-ratelimit-after", wait_time.into());
        //             headers.insert("retry-after", wait_time.into());

        //             let error_response = self.error_handler()(GovernorError::TooManyRequests {
        //                 wait_time,
        //                 headers: Some(headers),
        //             });

        //             ResponseFuture {
        //                 inner: Kind::Error {
        //                     error_response: Some(error_response),
        //                 },
        //             }
        //         }
        //     },

        //     Err(e) => {
        //         let error_response = self.error_handler()(e);
        //         ResponseFuture {
        //             inner: Kind::Error {
        //                 error_response: Some(error_response),
        //             },
        //         }
        //     }
        // }
    }
}

pub struct GovernorLayer {
    period: Duration,
    burst_size: u32,
}

impl GovernorLayer {
    #[must_use]
    pub fn new(period: Duration, burst_size: u32) -> Self {
        Self { period, burst_size }
    }
}

impl<S> Layer<S> for GovernorLayer {
    type Service = Governor<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Governor::new(inner, self.period, self.burst_size)
    }
}
