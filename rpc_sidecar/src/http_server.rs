use std::{
    collections::HashMap,
    net::IpAddr,
    num::{NonZeroU32, NonZeroU64},
    sync::Arc,
};

use casper_event_types::SidecarEvent;
use casper_json_rpc::{
    ConfigLimit, CorsOrigin, JsonRpcOptions, MethodLimiter, RequestHandlersBuilder,
};
use tokio::sync::broadcast::Sender as BroadcastSender;

use super::rpcs::{
    RpcWithOptionalParams, RpcWithParams, RpcWithoutParams,
    account::{PutDeploy, PutTransaction},
    chain::{
        GetBlock, GetBlockTransfers, GetEraInfoBySwitchBlock, GetEraSummary, GetStateRootHash,
    },
    docs::RpcDiscover,
    eth::{
        BlockNumber as EthBlockNumber, Call as EthCall, ChainId as EthChainId,
        ClientVersion as Web3ClientVersion, EstimateGas as EthEstimateGas, EthFilterState,
        FeeHistory as EthFeeHistory, GasPrice as EthGasPrice, GetBalance as EthGetBalance,
        GetBlockByHash as EthGetBlockByHash, GetBlockByNumber as EthGetBlockByNumber,
        GetCode as EthGetCode, GetFilterChanges as EthGetFilterChanges,
        GetFilterLogs as EthGetFilterLogs, GetLogs as EthGetLogs, GetStorageAt as EthGetStorageAt,
        GetTransactionByHash as EthGetTransactionByHash,
        GetTransactionCount as EthGetTransactionCount,
        GetTransactionReceipt as EthGetTransactionReceipt,
        MaxPriorityFeePerGas as EthMaxPriorityFeePerGas, NetVersion as EthNetVersion,
        NewFilter as EthNewFilter, SUBSCRIBE_METHOD, SendRawTransaction as EthSendRawTransaction,
        Syncing as EthSyncing, UNSUBSCRIBE_METHOD, UninstallFilter as EthUninstallFilter,
    },
    info::{GetChainspec, GetDeploy, GetValidatorChanges},
    state::{
        GetAccountInfo, GetAuctionInfo, GetBalance, GetDictionaryItem, GetItem, GetTrie,
        QueryBalance, QueryGlobalState,
    },
    state_get_auction_info_v2::GetAuctionInfo as GetAuctionInfoV2,
};
use crate::{
    NodeClient,
    rpcs::{
        info::{GetPeers, GetReward, GetStatus, GetTransaction},
        state::{GetAddressableEntity, GetPackage, QueryBalanceDetails},
    },
};

/// The URL path for all JSON-RPC requests.
const RPC_API_PATH: &str = "rpc";
const RPC_API_SERVER_NAME: &str = "JSON RPC";

/// Run the JSON-RPC server.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    node: Arc<dyn NodeClient>,
    sidecar_event_sender: BroadcastSender<SidecarEvent>,
    ip_address: IpAddr,
    port: u16,
    default_limit: ConfigLimit,
    mut limits: HashMap<String, ConfigLimit>,
    qps_limit: NonZeroU32,
    max_body_bytes: u64,
    max_batch_items: NonZeroU32,
    max_batch_response_bytes: NonZeroU64,
    max_eth_log_block_range: u64,
    cors_origin: String,
) {
    let mut handlers = RequestHandlersBuilder::new();
    let eth_filter_state = Arc::new(EthFilterState::new());
    let eth_syncing_state = Arc::new(EthSyncing::new());

    macro_rules! register_with_context {
        ($rpc:ident, $($context:expr),+ $(,)?) => {{
            let limit = limits.remove($rpc::METHOD).unwrap_or(default_limit.clone());
            $rpc::register_as_handler($($context,)* &mut handlers, limit);
        }};
    }

    macro_rules! register {
        ($rpc:ident) => {
            register_with_context!($rpc, node.clone());
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
    register!(Web3ClientVersion);
    register!(EthChainId);
    register!(EthNetVersion);
    register!(EthGasPrice);
    register!(EthMaxPriorityFeePerGas);
    register!(EthFeeHistory);
    register!(EthBlockNumber);
    register_with_context!(EthSyncing, eth_syncing_state.clone(), node.clone());
    register!(EthGetBlockByHash);
    register!(EthGetBlockByNumber);
    register!(EthGetBalance);
    register!(EthGetCode);
    register!(EthGetStorageAt);
    register!(EthGetTransactionCount);
    register!(EthSendRawTransaction);
    register!(EthGetTransactionByHash);
    register!(EthGetTransactionReceipt);
    register_with_context!(EthGetLogs, node.clone(), max_eth_log_block_range);
    register!(EthCall);
    register!(EthEstimateGas);
    register_with_context!(
        EthNewFilter,
        node.clone(),
        eth_filter_state.clone(),
        max_eth_log_block_range
    );
    register_with_context!(
        EthGetFilterChanges,
        node.clone(),
        eth_filter_state.clone(),
        max_eth_log_block_range
    );
    register_with_context!(
        EthGetFilterLogs,
        node.clone(),
        eth_filter_state.clone(),
        max_eth_log_block_range
    );
    register_with_context!(EthUninstallFilter, eth_filter_state.clone());

    let subscribe_limit = limits
        .remove(SUBSCRIBE_METHOD)
        .unwrap_or(default_limit.clone());
    let unsubscribe_limit = limits
        .remove(UNSUBSCRIBE_METHOD)
        .unwrap_or(default_limit.clone());
    let subscribe_limiter = MethodLimiter::new(&subscribe_limit);
    let unsubscribe_limiter = MethodLimiter::new(&unsubscribe_limit);

    let handlers = handlers.build();

    let cors_header = match cors_origin.as_str() {
        "" => None,
        "*" => Some(CorsOrigin::Any),
        _ => Some(CorsOrigin::Specified(cors_origin)),
    };
    let json_rpc_options = JsonRpcOptions {
        allow_unknown_fields: true,
        max_batch_items,
        max_batch_response_bytes,
    };
    super::rpcs::run_with_websocket(
        ip_address,
        port,
        handlers,
        node,
        sidecar_event_sender,
        qps_limit,
        max_body_bytes,
        json_rpc_options,
        RPC_API_PATH,
        RPC_API_SERVER_NAME,
        max_eth_log_block_range,
        cors_header,
        subscribe_limiter,
        unsubscribe_limiter,
    )
    .await;
}
