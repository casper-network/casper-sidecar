//! The set of JSON-RPCs which the API server handles.

use std::{convert::TryFrom, fmt, net::IpAddr, num::NonZeroU32, str, sync::Arc, time::Duration};

pub mod account;
pub mod chain;
mod common;
pub mod docs;
mod error;
mod error_code;
pub mod eth;
pub mod info;
pub mod speculative_exec;
pub mod speculative_open_rpc_schema;
pub mod state;
pub(crate) mod state_get_auction_info_v2;
#[cfg(test)]
pub(crate) mod test_utils;
mod types;

use async_trait::async_trait;
use governor::{
    DefaultDirectRateLimiter, Quota,
    clock::{Clock, DefaultClock},
};
use http::{
    StatusCode,
    header::{ACCEPT_ENCODING, RETRY_AFTER},
};
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as SerdeError};
use serde_json::{Value, json};
use tokio::sync::oneshot;
use tracing::{debug, info};
use warp::{
    Filter,
    filters::BoxedFilter,
    reject::{Reject, Rejection},
    reply::{self, Reply},
};

use casper_event_types::SidecarEvent;
use casper_json_rpc::{
    ConfigLimit, CorsOrigin, Error as RpcError, JsonRpcOptions, MethodLimiter, Params,
    RequestHandlers, RequestHandlersBuilder, ReservedErrorCode,
};
use casper_types::SemVer;
use tokio::sync::broadcast::Sender as BroadcastSender;

pub use common::ErrorData;
use docs::DocExample;
pub use error::Error;
pub use error_code::ErrorCode;

use crate::{ClientError, NodeClient};

pub const CURRENT_API_VERSION: ApiVersion = ApiVersion(SemVer::new(2, 0, 0));

/// This setting causes the server to ignore extra fields in JSON-RPC requests other than the
/// standard 'id', 'jsonrpc', 'method', and 'params' fields.
///
/// It will be changed to `false` for casper-node v2.0.0.
#[cfg(test)]
const ALLOW_UNKNOWN_FIELDS_IN_JSON_RPC_REQUEST: bool = true;

/// A JSON-RPC requiring the "params" field to be present.
#[async_trait]
pub(super) trait RpcWithParams {
    /// The JSON-RPC "method" name.
    const METHOD: &'static str;

    /// The JSON-RPC request's "params" type.
    type RequestParams: Serialize
        + for<'de> Deserialize<'de>
        + JsonSchema
        + DocExample
        + Send
        + 'static;

    /// The JSON-RPC response's "result" type.
    type ResponseResult: Serialize
        + for<'de> Deserialize<'de>
        + PartialEq
        + JsonSchema
        + DocExample
        + Send
        + 'static;

    /// Tries to parse the incoming JSON-RPC request's "params" field as `RequestParams`.
    fn try_parse_params(maybe_params: Option<Params>) -> Result<Self::RequestParams, RpcError> {
        let params = match maybe_params {
            Some(params) => Value::from(params),
            None => {
                return Err(RpcError::new(
                    ReservedErrorCode::InvalidParams,
                    "Missing 'params' field",
                ));
            }
        };
        serde_json::from_value::<Self::RequestParams>(params).map_err(|error| {
            RpcError::new(
                ReservedErrorCode::InvalidParams,
                format!("Failed to parse 'params' field: {error}"),
            )
        })
    }

    /// Registers this RPC as the handler for JSON-RPC requests whose "method" field is the same as
    /// `Self::METHOD`.
    fn register_as_handler(
        node_client: Arc<dyn NodeClient>,
        handlers_builder: &mut RequestHandlersBuilder,
        limit: ConfigLimit,
    ) {
        let handler = move |maybe_params| {
            let node_client = Arc::clone(&node_client);
            async move {
                let params = Self::try_parse_params(maybe_params)?;
                let res = Self::do_handle_request(node_client, params).await;
                match &res {
                    Ok(_) => {}
                    Err(err) => {
                        debug!(error=?err, method=Self::METHOD, "Error when handling request.");
                    }
                }
                res
            }
        };
        handlers_builder.register_handler(Self::METHOD, handler, &limit);
    }

    /// Tries to parse the params, and on success, returns the doc example, regardless of the value
    /// of the parsed params.
    #[cfg(test)]
    fn register_as_test_handler(handlers_builder: &mut RequestHandlersBuilder) {
        let handler = move |maybe_params| async move {
            let _params = Self::try_parse_params(maybe_params)?;
            Ok(Self::ResponseResult::doc_example())
        };
        handlers_builder.register_handler(Self::METHOD, handler, &ConfigLimit::default());
    }

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: Self::RequestParams,
    ) -> Result<Self::ResponseResult, RpcError>;
}

/// A JSON-RPC requiring the "params" field to be absent.
#[async_trait]
pub(super) trait RpcWithoutParams {
    /// The JSON-RPC "method" name.
    const METHOD: &'static str;

    /// The JSON-RPC response's "result" type.
    type ResponseResult: Serialize
        + for<'de> Deserialize<'de>
        + PartialEq
        + JsonSchema
        + DocExample
        + Send
        + 'static;

    /// Returns an error if the incoming JSON-RPC request's "params" field is not `None` or an empty
    /// Array or Object.
    fn check_no_params(maybe_params: Option<Params>) -> Result<(), RpcError> {
        if !maybe_params.unwrap_or_default().is_empty() {
            return Err(RpcError::new(
                ReservedErrorCode::InvalidParams,
                "'params' field should be an empty Array '[]', an empty Object '{}' or absent",
            ));
        }
        Ok(())
    }

    /// Registers this RPC as the handler for JSON-RPC requests whose "method" field is the same as
    /// `Self::METHOD`.
    fn register_as_handler(
        node_client: Arc<dyn NodeClient>,
        handlers_builder: &mut RequestHandlersBuilder,
        limit: ConfigLimit,
    ) {
        let handler = move |maybe_params| {
            let node_client = Arc::clone(&node_client);
            async move {
                Self::check_no_params(maybe_params)?;
                let res = Self::do_handle_request(node_client).await;
                if let Err(err) = &res {
                    debug!(error=?err, method=Self::METHOD, "Error when handling request.");
                }
                res
            }
        };
        handlers_builder.register_handler(Self::METHOD, handler, &limit);
    }

    /// Checks the params, and on success, returns the doc example.
    #[cfg(test)]
    fn register_as_test_handler(handlers_builder: &mut RequestHandlersBuilder) {
        let handler = move |maybe_params| async move {
            Self::check_no_params(maybe_params)?;
            Ok(Self::ResponseResult::doc_example())
        };
        handlers_builder.register_handler(Self::METHOD, handler, &ConfigLimit::default());
    }

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
    ) -> Result<Self::ResponseResult, RpcError>;
}

/// A JSON-RPC where the "params" field is optional.
///
/// Note that "params" being an empty JSON Array or empty JSON Object is treated the same as if
/// the "params" field is absent - i.e. it represents the `None` case.
#[async_trait]
pub(super) trait RpcWithOptionalParams {
    /// The JSON-RPC "method" name.
    const METHOD: &'static str;

    /// The JSON-RPC request's "params" type.  This will be passed to the handler wrapped in an
    /// `Option`.
    type OptionalRequestParams: Serialize
        + for<'de> Deserialize<'de>
        + JsonSchema
        + DocExample
        + Send
        + 'static;

    /// The JSON-RPC response's "result" type.
    type ResponseResult: Serialize
        + for<'de> Deserialize<'de>
        + PartialEq
        + JsonSchema
        + DocExample
        + Send
        + 'static;

    /// Tries to parse the incoming JSON-RPC request's "params" field as
    /// `Option<OptionalRequestParams>`.
    fn try_parse_params(
        maybe_params: Option<Params>,
    ) -> Result<Option<Self::OptionalRequestParams>, RpcError> {
        let params = match maybe_params {
            Some(params) => {
                if params.is_empty() {
                    Value::Null
                } else {
                    Value::from(params)
                }
            }
            None => Value::Null,
        };
        serde_json::from_value::<Option<Self::OptionalRequestParams>>(params).map_err(|error| {
            RpcError::new(
                ReservedErrorCode::InvalidParams,
                format!("Failed to parse 'params' field: {error}"),
            )
        })
    }

    /// Registers this RPC as the handler for JSON-RPC requests whose "method" field is the same as
    /// `Self::METHOD`.
    fn register_as_handler(
        node_client: Arc<dyn NodeClient>,
        handlers_builder: &mut RequestHandlersBuilder,
        limit: ConfigLimit,
    ) {
        let handler = move |maybe_params| {
            let node_client = Arc::clone(&node_client);
            async move {
                let params = Self::try_parse_params(maybe_params)?;
                let res = Self::do_handle_request(node_client, params).await;
                match &res {
                    Ok(_) => {}
                    Err(err) => {
                        debug!(error=?err, method=Self::METHOD, "Error when handling request.");
                    }
                }
                res
            }
        };
        handlers_builder.register_handler(Self::METHOD, handler, &limit);
    }

    /// Tries to parse the params, and on success, returns the doc example, regardless of the value
    /// of the parsed params.
    #[cfg(test)]
    fn register_as_test_handler(handlers_builder: &mut RequestHandlersBuilder) {
        let handler = move |maybe_params| async move {
            let _params = Self::try_parse_params(maybe_params)?;
            Ok(Self::ResponseResult::doc_example())
        };
        handlers_builder.register_handler(Self::METHOD, handler, &ConfigLimit::default());
    }

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: Option<Self::OptionalRequestParams>,
    ) -> Result<Self::ResponseResult, RpcError>;
}

#[derive(Debug)]
struct TooManyRequests(Duration);

impl Reject for TooManyRequests {}

async fn handle_rejection(error: Rejection) -> Result<impl Reply, Rejection> {
    if let Some(rejection) = error.find::<TooManyRequests>() {
        let response = reply::with_status(
            reply::json(&json!({ "message": "Too many requests" })),
            StatusCode::TOO_MANY_REQUESTS,
        );
        // Retry-After returns decimal integer indicating the seconds to delay after the response
        // is received. Add one second to the wait time to round up and be on the safe side.
        Ok(reply::with_header(
            response,
            RETRY_AFTER,
            (rejection.0.as_secs() + 1).to_string(),
        ))
    } else {
        Err(error)
    }
}

/// The actual service runner, common to `run()` and `run_with_cors()`.
async fn run_service(
    ip_address: IpAddr,
    port: u16,
    service_routes: BoxedFilter<(impl Reply + 'static,)>,
    server_name: &'static str,
    qps_limit: NonZeroU32,
) {
    let limiter = Arc::new(DefaultDirectRateLimiter::direct(Quota::per_second(
        qps_limit,
    )));

    let requrst_limit = warp::any()
        .and_then(move || {
            let limiter = limiter.clone();
            async move {
                if let Err(negative) = limiter.check() {
                    let wait_time = negative.wait_time_from(DefaultClock::default().now());
                    Err(warp::reject::custom(TooManyRequests(wait_time)))
                } else {
                    Ok(())
                }
            }
        })
        .untuple_one();

    // Supports content negotiation for gzip responses. This is an interim fix until
    // https://github.com/seanmonstar/warp/pull/513 moves forward.
    let service_routes_gzip = warp::header::exact(ACCEPT_ENCODING.as_str(), "gzip")
        .and(service_routes.clone())
        .with(warp::compression::gzip());

    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let address = std::net::SocketAddr::new(ip_address, port);
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .unwrap_or_else(|error| {
            panic!("failed to bind {server_name} server to {address}: {error}")
        });
    let address = listener.local_addr().expect("bound RPC listener address");
    let server = warp::serve(
        requrst_limit
            .and(service_routes_gzip.or(service_routes.clone()))
            .recover(handle_rejection),
    )
    .incoming(listener)
    .graceful(async {
        shutdown_receiver.await.ok();
    });
    info!(address = %address, "started {server_name} server");

    let _ = tokio::spawn(async move { server.run().await }).await;
    let _ = shutdown_sender.send(());
    info!("{server_name} server shut down");
}

/// Start JSON RPC server with CORS enabled in a background.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_with_cors(
    ip_address: IpAddr,
    port: u16,
    handlers: RequestHandlers,
    qps_limit: NonZeroU32,
    max_body_bytes: u64,
    json_rpc_options: JsonRpcOptions,
    api_path: &'static str,
    server_name: &'static str,
    cors_header: CorsOrigin,
) {
    let service_routes = casper_json_rpc::route_with_cors(
        api_path,
        max_body_bytes,
        handlers,
        json_rpc_options,
        cors_header,
    );
    run_service(ip_address, port, service_routes, server_name, qps_limit).await;
}

/// Start JSON RPC server with WebSocket support in a background.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_with_websocket(
    ip_address: IpAddr,
    port: u16,
    handlers: RequestHandlers,
    node_client: Arc<dyn NodeClient>,
    sidecar_event_sender: BroadcastSender<SidecarEvent>,
    qps_limit: NonZeroU32,
    max_body_bytes: u64,
    json_rpc_options: JsonRpcOptions,
    api_path: &'static str,
    server_name: &'static str,
    max_eth_log_block_range: u64,
    cors_header: Option<CorsOrigin>,
    subscribe_limiter: MethodLimiter,
    unsubscribe_limiter: MethodLimiter,
) {
    let websocket_origin_policy =
        eth::WebSocketOriginPolicy::from_cors_header(cors_header.as_ref());
    let http_routes = match cors_header {
        Some(cors_header) => casper_json_rpc::route_with_cors(
            api_path,
            max_body_bytes,
            handlers.clone(),
            json_rpc_options,
            cors_header,
        )
        .map(Reply::into_response)
        .boxed(),
        None => {
            casper_json_rpc::route(api_path, max_body_bytes, handlers.clone(), json_rpc_options)
                .map(Reply::into_response)
                .boxed()
        }
    };
    let websocket_routes = eth::websocket_route(
        api_path,
        handlers,
        node_client,
        sidecar_event_sender,
        json_rpc_options,
        websocket_origin_policy,
        max_eth_log_block_range,
        max_body_bytes,
        subscribe_limiter,
        unsubscribe_limiter,
    );
    let service_routes = websocket_routes.or(http_routes).unify().boxed();
    run_service(ip_address, port, service_routes, server_name, qps_limit).await;
}

/// Start JSON RPC server in a background.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run(
    ip_address: IpAddr,
    port: u16,
    handlers: RequestHandlers,
    qps_limit: NonZeroU32,
    max_body_bytes: u64,
    json_rpc_options: JsonRpcOptions,
    api_path: &'static str,
    server_name: &'static str,
) {
    let service_routes =
        casper_json_rpc::route(api_path, max_body_bytes, handlers, json_rpc_options);
    run_service(ip_address, port, service_routes, server_name, qps_limit).await;
}

#[derive(Clone, Debug, Default, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct ApiVersion(SemVer);

impl Serialize for ApiVersion {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            let str = format!("{}.{}.{}", self.0.major, self.0.minor, self.0.patch);
            String::serialize(&str, serializer)
        } else {
            self.0.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for ApiVersion {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let semver = if deserializer.is_human_readable() {
            let value_as_string = String::deserialize(deserializer)?;
            SemVer::try_from(value_as_string.as_str()).map_err(SerdeError::custom)?
        } else {
            SemVer::deserialize(deserializer)?
        };
        Ok(ApiVersion(semver))
    }
}

impl fmt::Display for ApiVersion {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use http_body_util::BodyExt;
    use warp::{Filter, Reply, filters::BoxedFilter};

    use casper_json_rpc::{Response, filters};
    use casper_types::DeployHash;

    use super::*;

    async fn send_request(
        method: &str,
        maybe_params: Option<Value>,
        filter: &BoxedFilter<(impl Reply + 'static,)>,
    ) -> Response {
        let mut body = json!({"jsonrpc": "2.0", "id": "a", "method": method});
        if let Some(params) = maybe_params {
            body["params"] = params;
        }

        let http_response = warp::test::request()
            .body(body.to_string())
            .filter(filter)
            .await
            .unwrap()
            .into_response();

        assert_eq!(http_response.status(), StatusCode::OK);
        let body_bytes = http_response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes();
        serde_json::from_slice(&body_bytes).unwrap()
    }

    #[tokio::test]
    async fn gzip_compresses_batch_responses() {
        let mut handlers = RequestHandlersBuilder::new();
        handlers.register_handler(
            "echo",
            |params| async move { Ok(params.map(Value::from).unwrap_or(Value::Null)) },
            &ConfigLimit::default(),
        );
        let service_routes = filters::main_filter(handlers.build(), JsonRpcOptions::default());
        let service_routes_gzip = warp::header::exact(ACCEPT_ENCODING.as_str(), "gzip")
            .and(service_routes)
            .with(warp::compression::gzip());
        let response = warp::test::request()
            .header(ACCEPT_ENCODING.as_str(), "gzip")
            .body(
                json!([
                    {"jsonrpc":"2.0","method":"echo","params":[1],"id":1},
                    {"jsonrpc":"2.0","method":"echo","params":[2],"id":2}
                ])
                .to_string(),
            )
            .filter(&service_routes_gzip)
            .await
            .unwrap()
            .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-encoding"], "gzip");
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert!(!body.is_empty());
    }

    mod rpc_with_params {
        use super::*;
        use crate::rpcs::info::{GetDeploy, GetDeployParams, GetDeployResult};

        fn main_filter_with_recovery() -> BoxedFilter<(impl Reply,)> {
            let mut handlers = RequestHandlersBuilder::new();
            GetDeploy::register_as_test_handler(&mut handlers);
            let handlers = handlers.build();

            filters::main_filter(
                handlers,
                JsonRpcOptions {
                    allow_unknown_fields: ALLOW_UNKNOWN_FIELDS_IN_JSON_RPC_REQUEST,
                    ..JsonRpcOptions::default()
                },
            )
            .recover(filters::handle_rejection)
            .boxed()
        }

        #[tokio::test]
        async fn should_parse_params() {
            let filter = main_filter_with_recovery();

            let params = serde_json::to_value(GetDeployParams {
                deploy_hash: DeployHash::default(),
                finalized_approvals: false,
            })
            .unwrap();
            let rpc_response = send_request(GetDeploy::METHOD, Some(params), &filter).await;
            assert_eq!(
                rpc_response.result().as_ref(),
                Some(GetDeployResult::doc_example())
            );
        }

        #[tokio::test]
        async fn should_return_error_if_missing_params() {
            let filter = main_filter_with_recovery();

            let rpc_response = send_request(GetDeploy::METHOD, None, &filter).await;
            assert_eq!(
                rpc_response.error().unwrap(),
                &RpcError::new(ReservedErrorCode::InvalidParams, "Missing 'params' field")
            );

            let rpc_response = send_request(GetDeploy::METHOD, Some(json!([])), &filter).await;
            assert_eq!(
                rpc_response.error().unwrap(),
                &RpcError::new(
                    ReservedErrorCode::InvalidParams,
                    "Failed to parse 'params' field: invalid length 0, expected struct \
                    GetDeployParams with 2 elements"
                )
            );
        }

        #[tokio::test]
        async fn should_return_error_on_failure_to_parse_params() {
            let filter = main_filter_with_recovery();

            let rpc_response = send_request(GetDeploy::METHOD, Some(json!([3])), &filter).await;
            assert_eq!(
                rpc_response.error().unwrap(),
                &RpcError::new(
                    ReservedErrorCode::InvalidParams,
                    "Failed to parse 'params' field: invalid type: number, expected a string"
                )
            );
        }
    }

    mod rpc_without_params {
        use casper_json_rpc::{DEFAULT_LIMIT_PERIOD, DEFAULT_LIMIT_REQUESTS, RpcErrorCode};

        use super::*;
        use crate::rpcs::info::{GetPeers, GetPeersResult};

        fn main_filter_with_recovery() -> BoxedFilter<(impl Reply,)> {
            let mut handlers = RequestHandlersBuilder::new();
            GetPeers::register_as_test_handler(&mut handlers);
            let handlers = handlers.build();

            filters::main_filter(
                handlers,
                JsonRpcOptions {
                    allow_unknown_fields: ALLOW_UNKNOWN_FIELDS_IN_JSON_RPC_REQUEST,
                    ..JsonRpcOptions::default()
                },
            )
            .recover(filters::handle_rejection)
            .boxed()
        }

        #[tokio::test]
        async fn should_check_no_params() {
            let filter = main_filter_with_recovery();

            let rpc_response = send_request(GetPeers::METHOD, None, &filter).await;
            assert_eq!(
                rpc_response.result().as_ref(),
                Some(GetPeersResult::doc_example())
            );

            let rpc_response = send_request(GetPeers::METHOD, Some(json!([])), &filter).await;
            assert_eq!(
                rpc_response.result().as_ref(),
                Some(GetPeersResult::doc_example())
            );

            let rpc_response = send_request(GetPeers::METHOD, Some(json!({})), &filter).await;
            assert_eq!(
                rpc_response.result().as_ref(),
                Some(GetPeersResult::doc_example())
            );
        }

        #[tokio::test]
        async fn should_return_error_if_params_not_empty() {
            let filter = main_filter_with_recovery();

            let rpc_response = send_request(GetPeers::METHOD, Some(json!([3])), &filter).await;
            assert_eq!(
                rpc_response.error().unwrap(),
                &RpcError::new(
                    ReservedErrorCode::InvalidParams,
                    "'params' field should be an empty Array '[]', an empty Object '{}' or absent"
                )
            );
        }

        #[tokio::test]
        async fn reach_rate_limit() {
            let filter = main_filter_with_recovery();

            for _ in 0..DEFAULT_LIMIT_REQUESTS.into() {
                let rpc_response = send_request(GetPeers::METHOD, None, &filter).await;
                assert!(rpc_response.is_success());
            }

            let rpc_response = send_request(GetPeers::METHOD, None, &filter).await;
            assert_eq!(
                rpc_response.error().unwrap().code(),
                RpcErrorCode::RequestThrottled as i64
            );

            // Wait for the limit period to expire.
            tokio::time::sleep(DEFAULT_LIMIT_PERIOD.into()).await;

            let rpc_response = send_request(GetPeers::METHOD, None, &filter).await;
            assert!(rpc_response.is_success());
        }
    }

    mod rpc_with_optional_params {
        use casper_types::BlockIdentifier;

        use crate::rpcs::chain::{GetBlock, GetBlockParams, GetBlockResult};

        use super::*;

        fn main_filter_with_recovery() -> BoxedFilter<(impl Reply,)> {
            let mut handlers = RequestHandlersBuilder::new();
            GetBlock::register_as_test_handler(&mut handlers);
            let handlers = handlers.build();

            filters::main_filter(
                handlers,
                JsonRpcOptions {
                    allow_unknown_fields: ALLOW_UNKNOWN_FIELDS_IN_JSON_RPC_REQUEST,
                    ..JsonRpcOptions::default()
                },
            )
            .recover(filters::handle_rejection)
            .boxed()
        }

        #[tokio::test]
        async fn should_parse_without_params() {
            let filter = main_filter_with_recovery();

            let rpc_response = send_request(GetBlock::METHOD, None, &filter).await;
            assert_eq!(
                rpc_response.result().as_ref(),
                Some(GetBlockResult::doc_example())
            );

            let rpc_response = send_request(GetBlock::METHOD, Some(json!([])), &filter).await;
            assert_eq!(
                rpc_response.result().as_ref(),
                Some(GetBlockResult::doc_example())
            );

            let rpc_response = send_request(GetBlock::METHOD, Some(json!({})), &filter).await;
            assert_eq!(
                rpc_response.result().as_ref(),
                Some(GetBlockResult::doc_example())
            );
        }

        #[tokio::test]
        async fn should_parse_with_params() {
            let filter = main_filter_with_recovery();

            let params = serde_json::to_value(GetBlockParams {
                block_identifier: BlockIdentifier::Height(1),
            })
            .unwrap();

            let rpc_response = send_request(GetBlock::METHOD, Some(params), &filter).await;
            assert_eq!(
                rpc_response.result().as_ref(),
                Some(GetBlockResult::doc_example())
            );
        }

        #[tokio::test]
        async fn should_return_error_on_failure_to_parse_params() {
            let filter = main_filter_with_recovery();

            let rpc_response = send_request(GetBlock::METHOD, Some(json!(["a"])), &filter).await;
            assert_eq!(
                rpc_response.error().unwrap(),
                &RpcError::new(
                    ReservedErrorCode::InvalidParams,
                    "Failed to parse 'params' field: unknown variant `a`, expected `Hash` or \
                    `Height`"
                )
            );
        }
    }
}
