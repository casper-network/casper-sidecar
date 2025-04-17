use std::{collections::HashMap, net::IpAddr, num::NonZeroU32, sync::Arc};

use casper_json_rpc::{ConfigLimit, CorsOrigin, RequestHandlersBuilder};

use crate::{
    node_client::NodeClient,
    rpcs::{
        RpcWithParams, RpcWithoutParams,
        speculative_exec::{SpeculativeExec, SpeculativeExecTxn, SpeculativeRpcDiscover},
    },
};

/// The URL path for all JSON-RPC requests.
pub const SPECULATIVE_EXEC_API_PATH: &str = "rpc";

pub const SPECULATIVE_EXEC_SERVER_NAME: &str = "speculative execution";

/// Run the speculative execution server.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    node: Arc<dyn NodeClient>,
    ip_address: IpAddr,
    port: u16,
    default_limit: ConfigLimit,
    mut limits: HashMap<String, ConfigLimit>,
    qps_limit: NonZeroU32,
    max_body_bytes: u64,
    cors_origin: String,
) {
    let mut handlers = RequestHandlersBuilder::new();

    macro_rules! register {
        ($rpc:ident) => {
            let limit = limits.remove($rpc::METHOD).unwrap_or(default_limit.clone());
            $rpc::register_as_handler(node.clone(), &mut handlers, limit);
        };
    }

    register!(SpeculativeExecTxn);
    register!(SpeculativeExec);
    register!(SpeculativeRpcDiscover);

    let handlers = handlers.build();

    match cors_origin.as_str() {
        "" => {
            super::rpcs::run(
                ip_address,
                port,
                handlers,
                qps_limit,
                max_body_bytes,
                SPECULATIVE_EXEC_API_PATH,
                SPECULATIVE_EXEC_SERVER_NAME,
            )
            .await;
        }
        "*" => {
            super::rpcs::run_with_cors(
                ip_address,
                port,
                handlers,
                qps_limit,
                max_body_bytes,
                SPECULATIVE_EXEC_API_PATH,
                SPECULATIVE_EXEC_SERVER_NAME,
                CorsOrigin::Any,
            )
            .await;
        }
        _ => {
            super::rpcs::run_with_cors(
                ip_address,
                port,
                handlers,
                qps_limit,
                max_body_bytes,
                SPECULATIVE_EXEC_API_PATH,
                SPECULATIVE_EXEC_SERVER_NAME,
                CorsOrigin::Specified(cors_origin),
            )
            .await;
        }
    }
}
