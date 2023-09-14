use prometheus::{GaugeVec, HistogramOpts, HistogramVec, IntCounterVec, Opts, Registry};

#[cfg(feature = "db-perf-measurement")]
const DB_OPERATION_BUCKETS: &[f64; 8] = &[
    3e+5_f64, 3e+6_f64, 10e+6_f64, 20e+6_f64, 5e+7_f64, 1e+8_f64, 5e+8_f64, 1e+9_f64,
];
const BUCKETS: &[f64; 8] = &[
    5e+2_f64, 1e+3_f64, 2e+3_f64, 5e+3_f64, 5e+4_f64, 5e+5_f64, 5e+6_f64, 5e+7_f64,
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
    .expect("metric can't be created");
    pub static ref INTERNAL_EVENTS: IntCounterVec = IntCounterVec::new(
        Opts::new("internal_events", "Count of internal events"),
        &["category", "description"]
    )
    .expect("metric can't be created");

    pub static ref NODE_STATUSES: GaugeVec = GaugeVec::new(
        Opts::new("node_statuses", "Current status of node to which sidecar is connected. Numbers mean: 0 - preparing; 1 - connecting; 2 - connected; 3 - reconnecting; -1 - defunct (used up all connection attempts)"),
        &["node"]
    )
    .expect("metric can't be created");

}

#[cfg(feature = "db-perf-measurement")]
lazy_static! {
    pub static ref DB_OPERATION_TIMES: HistogramVec = HistogramVec::new(
        HistogramOpts {
            common_opts: Opts::new(
                "db_operation_times",
                "Times (in nanoseconds) it took to perform a database operation."
            ),
            buckets: Vec::from(DB_OPERATION_BUCKETS as &'static [f64]),
        },
        &["filter"]
    )
    .expect("metric can't be created");
}

pub fn register_metrics() {
    REGISTRY
        .register(Box::new(ERROR_COUNTS.clone()))
        .expect("cannot register metric");
    REGISTRY
        .register(Box::new(RECEIVED_BYTES.clone()))
        .expect("cannot register metric");
    REGISTRY
        .register(Box::new(INTERNAL_EVENTS.clone()))
        .expect("cannot register metric");
    REGISTRY
        .register(Box::new(NODE_STATUSES.clone()))
        .expect("cannot register metric");
    #[cfg(feature = "db-perf-measurement")]
    REGISTRY
        .register(Box::new(DB_OPERATION_TIMES.clone()))
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
