use std::fmt::{Display, Formatter};

use once_cell::sync::Lazy;
use prometheus::{IntCounterVec, Opts, Registry};

pub static REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);

static ERROR_COUNTS: Lazy<IntCounterVec> = Lazy::new(|| {
    let counter = IntCounterVec::new(
        Opts::new("error_counts", "Error counts"),
        &["category", "description"],
    )
    .unwrap();
    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("cannot register metric");
    counter
});

pub fn observe_error(category: &str, description: &str) {
    ERROR_COUNTS
        .with_label_values(&[category, description])
        .inc();
}

pub struct MetricCollectionError {
    reason: String,
}

impl Display for MetricCollectionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MetricCollectionError: {}", self.reason)
    }
}

impl MetricCollectionError {
    fn new(reason: String) -> Self {
        MetricCollectionError { reason }
    }
}

pub fn metrics_summary() -> Result<String, MetricCollectionError> {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();
    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&REGISTRY.gather(), &mut buffer) {
        return Err(MetricCollectionError::new(format!(
            "could not encode custom metrics: {e}"
        )));
    };
    let mut res = match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            return Err(MetricCollectionError::new(format!(
                "custom metrics have a non-utf8 character: {e}"
            )));
        }
    };
    buffer.clear();

    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&prometheus::gather(), &mut buffer) {
        return Err(MetricCollectionError::new(format!(
            "error when encoding default prometheus metrics: {e}"
        )));
    };
    let res_custom = match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            return Err(MetricCollectionError::new(format!(
                "default and custom metrics have a non-utf8 character: {e}"
            )))
        }
    };
    buffer.clear();
    res.push_str(&res_custom);
    Ok(res)
}
