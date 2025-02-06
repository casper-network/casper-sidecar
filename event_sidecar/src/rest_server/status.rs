use casper_event_types::SIDECAR_VERSION;
use casper_types::ProtocolVersion;
use http::StatusCode;
use serde::Serialize;
use warp::{reject::Rejection, reply::Reply, Filter};

#[derive(Clone, Debug, Serialize)]
struct SidecarStatus {
    version: ProtocolVersion,
}

pub fn status_filters() -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    warp::path!("status").and(warp::get()).and_then(get_status)
}

pub(super) async fn get_status() -> Result<impl Reply, Rejection> {
    let data = SidecarStatus {
        version: *SIDECAR_VERSION,
    };
    let json = warp::reply::json(&data);
    Ok(warp::reply::with_status(json, StatusCode::OK).into_response())
}
