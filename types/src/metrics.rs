use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, Opts, Registry};
const BUCKETS: &[f64; 11] = &[
    5000.0, 10000.0, 50000.0, 100000.0, 150000.0, 200000.0, 500000.0, 1000000.0, 2000000.0,
    3000000.0, 5000000.0,
];
lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();
    pub static ref ERROR_COUNTS: IntCounterVec = IntCounterVec::new(
        Opts::new("error_counts", "Error counts"),
        &["category", "description"]
    )
    .unwrap();
    pub static ref RECEIVED_BYTES: HistogramVec = HistogramVec::new(
        HistogramOpts {
            common_opts: Opts::new("received_bytes", "Received bytes"),
            buckets: Vec::from(BUCKETS as &'static [f64]),
        },
        &["filter"]
    )
    .expect("metric can be created");
}

pub fn register_metrics() {
    REGISTRY
        .register(Box::new(ERROR_COUNTS.clone()))
        .expect("cannot register metric");
    REGISTRY
        .register(Box::new(RECEIVED_BYTES.clone()))
        .expect("cannot register metric");
}
pub struct MetricCollectionError {
    reason: String,
}

impl ToString for MetricCollectionError {
    fn to_string(&self) -> String {
        format!("MetricCollectionError: {}", self.reason)
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
            "could not encode custom metrics: {}",
            e
        )));
    };
    let mut res = match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            return Err(MetricCollectionError::new(format!(
                "custom metrics have a non-utf8 character: {}",
                e
            )));
        }
    };
    buffer.clear();

    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&prometheus::gather(), &mut buffer) {
        return Err(MetricCollectionError::new(format!(
            "error when encoding default prometheus metrics: {}",
            e
        )));
    };
    let res_custom = match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            return Err(MetricCollectionError::new(format!(
                "Default and custom metrics have a non-utf8 character: {}",
                e
            )))
        }
    };
    buffer.clear();
    res.push_str(&res_custom);
    Ok(res)
}
