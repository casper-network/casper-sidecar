pub mod metrics;
pub use metrics::{MetricCollectionError, metrics_summary, observe_error};
pub mod db;
pub mod rest_api;
pub mod rpc;
pub mod sse;

use metrics::REGISTRY;
