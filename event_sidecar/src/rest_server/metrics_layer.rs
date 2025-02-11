use derive_new::new;
use futures::{ready, Future};
use http::{Request, Response};
use hyper::Body;
use metrics::rest_api::{dec_connected_clients, inc_connected_clients, observe_response_time};
use pin_project::{pin_project, pinned_drop};
use std::{
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll},
    time::Instant,
};
use tower::{Layer, Service};

/// Warp layer that records metrics for each request.
///
/// For now this supports only services that are bound by a Request<Body> as input and Response<Body> as output.
/// This is due to the fact that we use the Request's `uri()` and Responses `status()` to give context to the observed metrics.
#[derive(new, Debug, Clone)]
pub struct MetricsLayer {
    metrics_abstractor: fn(&str) -> String,
}

impl<S> Layer<S> for MetricsLayer {
    type Service = MetricsService<S>;

    fn layer(&self, service: S) -> Self::Service {
        MetricsService::new(service, self.metrics_abstractor)
    }
}

#[derive(Debug, Clone)]
pub struct MetricsService<S> {
    inner: S,
    metrics_abstractor: fn(&str) -> String,
}

impl<S> MetricsService<S> {
    pub fn new(inner: S, metrics_abstractor: fn(&str) -> String) -> Self {
        MetricsService {
            inner,
            metrics_abstractor,
        }
    }
}

impl<S, B> Service<Request<B>> for MetricsService<S>
where
    S: Service<Request<B>, Response = Response<Body>> + 'static,
{
    type Response = Response<Body>;
    type Error = S::Error;
    type Future = MetricsFuture<S::Future, S::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        let metrics_abstraction = (self.metrics_abstractor)(request.uri().path());
        let future = self.inner.call(request);
        MetricsFuture::new(future, metrics_abstraction)
    }
}

#[pin_project(PinnedDrop)]
pub struct MetricsFuture<F, ErrorType>
where
    F: Future<Output = Result<Response<Body>, ErrorType>>,
{
    #[pin]
    inner: F,
    #[pin]
    connection_end_observed: AtomicBool,
    start: Instant,
    metrics_category: String,
}

#[pinned_drop]
impl<T, X> PinnedDrop for MetricsFuture<T, X>
where
    T: Future<Output = Result<Response<Body>, X>>,
{
    fn drop(self: Pin<&mut Self>) {
        self.observe_end();
    }
}

impl<T, X> MetricsFuture<T, X>
where
    T: Future<Output = Result<Response<Body>, X>>,
{
    pub fn new(inner: T, metrics_category: String) -> Self {
        let start = Instant::now();
        inc_connected_clients();
        MetricsFuture {
            inner,
            connection_end_observed: AtomicBool::new(false),
            start,
            metrics_category,
        }
    }

    fn observe_end(self: Pin<&mut Self>) {
        let metrics_category = self.metrics_category.clone();
        let duration = self.start.elapsed();
        let got = self
            .project()
            .connection_end_observed
            .swap(true, Ordering::Relaxed);
        if !got {
            dec_connected_clients();
            observe_response_time(&metrics_category, "disconnected", duration);
        }
    }
}

impl<F, E> Future for MetricsFuture<F, E>
where
    F: Future<Output = Result<Response<Body>, E>>,
{
    type Output = Result<Response<Body>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let res = ready!(this.inner.poll(cx));
        let got = this.connection_end_observed.swap(true, Ordering::Relaxed);
        if !got {
            let metrics_category = this.metrics_category;
            let duration = this.start.elapsed();
            dec_connected_clients();
            match &res {
                Ok(r) => {
                    let status = r.status();
                    let status = status.as_str();
                    observe_response_time(metrics_category, status, duration);
                }
                Err(_) => observe_response_time(metrics_category, "error", duration),
            }
        }
        Poll::Ready(res)
    }
}
