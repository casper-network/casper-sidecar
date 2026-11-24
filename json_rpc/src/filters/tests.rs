use http_body_util::BodyExt;
mod base_filter_with_recovery_tests;
mod main_filter_with_recovery_tests;

use serde::Deserialize;

/// The HTTP response body returned in the event of a warp rejection.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResponseBodyOnRejection {
    message: String,
}

impl ResponseBodyOnRejection {
    async fn from_response(response: warp::reply::Response) -> Self {
        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&body_bytes).unwrap()
    }
}
