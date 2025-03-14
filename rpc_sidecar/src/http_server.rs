use std::sync::Arc;

use casper_json_rpc::{CorsOrigin, RequestHandlersBuilder};
use hyper::server::{conn::AddrIncoming, Builder};

use super::rpcs::{
    account::{PutDeploy, PutTransaction},
    chain::{
        GetBlock, GetBlockTransfers, GetEraInfoBySwitchBlock, GetEraSummary, GetStateRootHash,
    },
    docs::RpcDiscover,
    info::{GetChainspec, GetDeploy, GetValidatorChanges},
    state::{
        GetAccountInfo, GetAuctionInfo, GetBalance, GetDictionaryItem, GetItem, GetTrie,
        QueryBalance, QueryGlobalState,
    },
    state_get_auction_info_v2::GetAuctionInfo as GetAuctionInfoV2,
    RpcWithOptionalParams, RpcWithParams, RpcWithoutParams,
};
use crate::{
    rpcs::{
        info::{GetPeers, GetReward, GetStatus, GetTransaction},
        state::{GetAddressableEntity, GetPackage, QueryBalanceDetails},
    },
    NodeClient,
};

/// The URL path for all JSON-RPC requests.
const RPC_API_PATH: &str = "rpc";
const RPC_API_SERVER_NAME: &str = "JSON RPC";

/// Run the JSON-RPC server.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    node: Arc<dyn NodeClient>,
    builder: Builder<AddrIncoming>,
    qps_limit: u64,
    max_body_bytes: u64,
    cors_origin: String,
) {
    let mut handlers = RequestHandlersBuilder::new();

    macro_rules! register {
        ($rpc:ident) => {
            $rpc::register_as_handler(node.clone(), &mut handlers);
        };
    }

    register!(PutDeploy);
    register!(PutTransaction);
    register!(GetBlock);
    register!(GetBlockTransfers);
    register!(GetStateRootHash);
    register!(GetItem);
    register!(QueryGlobalState);
    register!(GetBalance);
    register!(GetAccountInfo);
    register!(GetAddressableEntity);
    register!(GetPackage);
    register!(GetDeploy);
    register!(GetTransaction);
    register!(GetPeers);
    register!(GetStatus);
    register!(GetReward);
    register!(GetEraInfoBySwitchBlock);
    register!(GetEraSummary);
    register!(GetAuctionInfo);
    register!(GetAuctionInfoV2);
    register!(GetTrie);
    register!(GetValidatorChanges);
    register!(RpcDiscover);
    register!(GetDictionaryItem);
    register!(GetChainspec);
    register!(QueryBalance);
    register!(QueryBalanceDetails);

    let handlers = handlers.build();

    match cors_origin.as_str() {
        "" => {
            super::rpcs::run(
                builder,
                handlers,
                qps_limit,
                max_body_bytes,
                RPC_API_PATH,
                RPC_API_SERVER_NAME,
            )
            .await;
        }
        "*" => {
            super::rpcs::run_with_cors(
                builder,
                handlers,
                qps_limit,
                max_body_bytes,
                RPC_API_PATH,
                RPC_API_SERVER_NAME,
                CorsOrigin::Any,
            )
            .await;
        }
        _ => {
            super::rpcs::run_with_cors(
                builder,
                handlers,
                qps_limit,
                max_body_bytes,
                RPC_API_PATH,
                RPC_API_SERVER_NAME,
                CorsOrigin::Specified(cors_origin),
            )
            .await;
        }
    }
}
