use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use casper_binary_port::PurseIdentifier;
use casper_json_rpc::{Error as RpcError, Params};
use casper_types::{EvmAddr, GlobalStateIdentifier, Key, StoredValue, U256, U512, evm};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    super::{Error as RpcServerError, NodeClient, RpcWithParams},
    config::read_evm_config,
    eth_u256::EthU256,
    types::{
        BlockNumberParam, BlockTag, EthAddress, PendingPolicy, StateBlockParam, internal_error,
        parse_positional_params,
    },
};
use crate::{ClientError, rpcs::docs::DocExample};

static GET_BALANCE_PARAMS_EXAMPLE: LazyLock<GetBalanceParams> =
    LazyLock::new(|| GetBalanceParams {
        address: EthAddress::from(evm::Address::ZERO),
        block: StateBlockParam::Number(BlockNumberParam::Tag(BlockTag::Latest)),
    });

/// Params for `eth_getBalance`.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct GetBalanceParams {
    address: EthAddress,
    block: StateBlockParam,
}

impl GetBalanceParams {
    fn address(&self) -> evm::Address {
        self.address.into_inner()
    }
}

impl DocExample for GetBalanceParams {
    fn doc_example() -> &'static Self {
        &GET_BALANCE_PARAMS_EXAMPLE
    }
}

#[derive(Deserialize)]
struct PositionalParams(EthAddress, StateBlockParam);

impl From<PositionalParams> for GetBalanceParams {
    fn from(params: PositionalParams) -> Self {
        GetBalanceParams {
            address: params.0,
            block: params.1,
        }
    }
}

async fn resolve_purse_identifier(
    node_client: &dyn NodeClient,
    state_identifier: Option<GlobalStateIdentifier>,
    address: evm::Address,
) -> Result<PurseIdentifier, RpcError> {
    let identity_key = Key::Evm(EvmAddr::Account(address));
    let maybe_identity = node_client
        .query_global_state(state_identifier, identity_key, vec![])
        .await
        .map_err(|error| RpcServerError::NodeRequest("EVM account identity", error))?;

    let identity = match maybe_identity.map(|value| value.into_inner().0) {
        Some(StoredValue::CLValue(cl_value)) => cl_value.into_t::<Key>().map_err(|error| {
            internal_error(format!(
                "invalid EVM account identity under {identity_key}: {error}"
            ))
        })?,
        Some(other) => {
            return Err(internal_error(format!(
                "expected EVM account identity CLValue under {identity_key}, found {}",
                other.type_name()
            )));
        }
        None => {
            return Ok(PurseIdentifier::Purse(evm::deterministic_purse(address)));
        }
    };

    match identity {
        Key::Account(account_hash) => Ok(PurseIdentifier::Account(account_hash)),
        Key::URef(purse) => Ok(PurseIdentifier::Purse(purse)),
        other => Err(internal_error(format!(
            "expected EVM account identity to reference an account or purse, found {}",
            other.type_string()
        ))),
    }
}

async fn read_available_balance(
    node_client: &dyn NodeClient,
    state_identifier: Option<GlobalStateIdentifier>,
    purse_identifier: PurseIdentifier,
) -> Result<U512, RpcError> {
    match node_client
        .read_balance(state_identifier, purse_identifier)
        .await
    {
        Ok(balance) => Ok(balance.available_balance),
        Err(ClientError::PurseNotFound) => Ok(U512::zero()),
        Err(error) => Err(RpcServerError::NodeRequest("EVM balance", error).into()),
    }
}

fn balance_in_wei(balance_motes: U512, wei_per_mote: u64) -> Result<EthU256, RpcError> {
    let balance_wei = balance_motes
        .checked_mul(U512::from(wei_per_mote))
        .ok_or_else(|| internal_error("EVM balance overflow while converting motes to wei"))?;
    if balance_wei.bits() > 256 {
        return Err(internal_error(
            "EVM balance in wei exceeds the Ethereum U256 range",
        ));
    }

    let mut bytes = [0u8; 64];
    balance_wei.to_big_endian(&mut bytes);
    Ok(EthU256::from(U256::from_big_endian(&bytes[32..])))
}

/// `eth_getBalance`.
pub struct GetBalance;

#[async_trait]
impl RpcWithParams for GetBalance {
    const METHOD: &'static str = "eth_getBalance";
    type RequestParams = GetBalanceParams;
    type ResponseResult = EthU256;

    fn try_parse_params(maybe_params: Option<Params>) -> Result<Self::RequestParams, RpcError> {
        parse_positional_params::<PositionalParams>(maybe_params).map(Into::into)
    }

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: GetBalanceParams,
    ) -> Result<EthU256, RpcError> {
        let state_identifier = params
            .block
            .resolve_state_identifier(node_client.as_ref(), PendingPolicy::Latest)
            .await?;
        let purse_identifier =
            resolve_purse_identifier(node_client.as_ref(), state_identifier, params.address())
                .await?;
        let balance =
            read_available_balance(node_client.as_ref(), state_identifier, purse_identifier)
                .await?;
        let evm_config = read_evm_config(node_client.as_ref()).await?;
        balance_in_wei(balance, evm_config.wei_per_mote)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};

    use casper_binary_port::{
        BalanceResponse, BinaryResponse, Command, ErrorCode as BinaryPortErrorCode, GetRequest,
        GlobalStateEntityQualifier, GlobalStateQueryResult, GlobalStateRequest, InformationRequest,
    };
    use casper_json_rpc::ReservedErrorCode;
    use casper_types::{
        AccessRights, Block, BlockIdentifier, CLValue, ChainspecRawBytes, TestBlockBuilder, URef,
        account::AccountHash, global_state::TrieMerkleProof, testing::TestRng,
    };
    use serde_json::json;

    use super::*;
    use crate::rpcs::{eth::types::BlockHashParam, test_utils::BinaryPortMock};

    const WEI_PER_MOTE: u64 = 1_000_000_000;

    fn address(byte: u8) -> evm::Address {
        evm::Address::new([byte; evm::ADDRESS_LENGTH])
    }

    fn state_request(
        state_identifier: Option<GlobalStateIdentifier>,
        qualifier: GlobalStateEntityQualifier,
    ) -> Command {
        Command::Get(GetRequest::State(Box::new(GlobalStateRequest::new(
            state_identifier,
            qualifier,
        ))))
    }

    async fn add_identity_response(
        client: &BinaryPortMock,
        state_identifier: Option<GlobalStateIdentifier>,
        address: evm::Address,
        maybe_identity: Option<StoredValue>,
    ) {
        client
            .when_then(
                state_request(
                    state_identifier,
                    GlobalStateEntityQualifier::Item {
                        base_key: Key::Evm(EvmAddr::Account(address)),
                        path: Vec::new(),
                    },
                ),
                BinaryResponse::from_option(
                    maybe_identity
                        .map(|identity| GlobalStateQueryResult::new(identity, Vec::new())),
                ),
            )
            .await;
    }

    fn balance_response(total_balance: U512, available_balance: U512) -> BalanceResponse {
        BalanceResponse {
            total_balance,
            available_balance,
            total_balance_proof: Box::new(TrieMerkleProof::new(
                Key::Balance([0; 32]),
                StoredValue::CLValue(CLValue::from_t(total_balance).unwrap()),
                VecDeque::new(),
            )),
            balance_holds: BTreeMap::new(),
        }
    }

    async fn add_balance_response(
        client: &BinaryPortMock,
        state_identifier: Option<GlobalStateIdentifier>,
        purse_identifier: PurseIdentifier,
        response: BinaryResponse,
    ) {
        client
            .when_then(
                state_request(
                    state_identifier,
                    GlobalStateEntityQualifier::Balance { purse_identifier },
                ),
                response,
            )
            .await;
    }

    fn chainspec(wei_per_mote: u64) -> ChainspecRawBytes {
        let toml = format!(
            r#"
[evm]
enabled = true
chain_id = 7
spec = "prague"
block_gas_limit = 30000000
base_fee = 1
wei_per_mote = {wei_per_mote}
transaction_lanes = [[100, 131072, 1024, 1000000, 50]]
"#
        );
        ChainspecRawBytes::new(toml.into_bytes().into(), None, None)
    }

    async fn add_chainspec_response(client: &BinaryPortMock, response: BinaryResponse) {
        let request = InformationRequest::ChainspecRawBytes
            .try_into()
            .expect("chainspec information request should convert");
        client.when_then(Command::Get(request), response).await;
    }

    #[tokio::test]
    async fn reads_linked_account_available_balance_at_height_and_scales_to_wei() {
        let client = BinaryPortMock::new();
        let evm_address = address(1);
        let account_hash = AccountHash::new([2; 32]);
        let state_identifier = Some(GlobalStateIdentifier::BlockHeight(42));
        let block = Block::V2(
            TestBlockBuilder::new()
                .height(42)
                .build(&mut TestRng::new()),
        );
        client
            .add_block_header_req_res(
                block.clone_header(),
                InformationRequest::BlockHeader(Some(casper_types::BlockIdentifier::Height(42))),
            )
            .await;
        add_identity_response(
            &client,
            state_identifier,
            evm_address,
            Some(StoredValue::CLValue(
                CLValue::from_t(Key::Account(account_hash)).unwrap(),
            )),
        )
        .await;
        add_balance_response(
            &client,
            state_identifier,
            PurseIdentifier::Account(account_hash),
            BinaryResponse::from_value(balance_response(U512::from(99u64), U512::from(12u64))),
        )
        .await;
        add_chainspec_response(&client, BinaryResponse::from_value(chainspec(WEI_PER_MOTE))).await;

        let result = GetBalance::do_handle_request(
            Arc::new(client),
            GetBalanceParams {
                address: EthAddress::from(evm_address),
                block: BlockNumberParam::Height(EthU256::from(42u64)).into(),
            },
        )
        .await
        .expect("balance lookup should succeed");

        assert_eq!(result, EthU256::from(12u64 * WEI_PER_MOTE));
    }

    #[tokio::test]
    async fn resolves_evm_native_identity_to_its_purse() {
        let client = BinaryPortMock::new();
        let evm_address = address(3);
        let purse = URef::new([4; 32], AccessRights::READ_ADD_WRITE);
        add_identity_response(
            &client,
            None,
            evm_address,
            Some(StoredValue::CLValue(
                CLValue::from_t(Key::URef(purse)).unwrap(),
            )),
        )
        .await;

        let result = resolve_purse_identifier(&client, None, evm_address)
            .await
            .expect("identity lookup should succeed");

        assert_eq!(result, PurseIdentifier::Purse(purse));
        client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn uses_deterministic_purse_when_identity_is_absent() {
        let client = BinaryPortMock::new();
        let evm_address = address(5);
        add_identity_response(&client, None, evm_address, None).await;

        let result = resolve_purse_identifier(&client, None, evm_address)
            .await
            .expect("missing identity should use deterministic purse");

        assert_eq!(
            result,
            PurseIdentifier::Purse(evm::deterministic_purse(evm_address))
        );
        client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn returns_zero_when_deterministic_purse_does_not_exist() {
        let client = BinaryPortMock::new();
        let evm_address = address(6);
        add_identity_response(&client, None, evm_address, None).await;
        add_balance_response(
            &client,
            None,
            PurseIdentifier::Purse(evm::deterministic_purse(evm_address)),
            BinaryResponse::new_error(casper_binary_port::ErrorCode::PurseNotFound),
        )
        .await;
        add_chainspec_response(&client, BinaryResponse::from_value(chainspec(WEI_PER_MOTE))).await;

        let result = GetBalance::do_handle_request(
            Arc::new(client),
            GetBalanceParams {
                address: EthAddress::from(evm_address),
                block: BlockNumberParam::Tag(BlockTag::Latest).into(),
            },
        )
        .await
        .expect("unknown account should have a zero balance");

        assert_eq!(result, EthU256::ZERO);
        assert_eq!(serde_json::to_value(result).unwrap(), json!("0x0"));
    }

    #[tokio::test]
    async fn rejects_malformed_identity_clvalue() {
        let client = BinaryPortMock::new();
        let evm_address = address(7);
        add_identity_response(
            &client,
            None,
            evm_address,
            Some(StoredValue::CLValue(CLValue::from_t(1u64).unwrap())),
        )
        .await;

        let error = resolve_purse_identifier(&client, None, evm_address)
            .await
            .expect_err("non-Key identity should fail");

        assert_eq!(error.code(), ReservedErrorCode::InternalError as i64);
        client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn rejects_identity_pointing_to_an_unsupported_key() {
        let client = BinaryPortMock::new();
        let evm_address = address(8);
        add_identity_response(
            &client,
            None,
            evm_address,
            Some(StoredValue::CLValue(
                CLValue::from_t(Key::Hash([9; 32])).unwrap(),
            )),
        )
        .await;

        let error = resolve_purse_identifier(&client, None, evm_address)
            .await
            .expect_err("unsupported identity key should fail");

        assert_eq!(error.code(), ReservedErrorCode::InternalError as i64);
        client.verify_no_lingering().await;
    }

    #[test]
    fn parses_numeric_height_and_rejects_missing_block_selector() {
        let encoded_address = format!("0x{}", "01".repeat(evm::ADDRESS_LENGTH));
        let params = GetBalance::try_parse_params(Some(Params::Array(vec![
            json!(encoded_address),
            json!("0x2a"),
        ])))
        .expect("numeric block selector should parse");
        assert_eq!(
            params.block,
            StateBlockParam::Number(BlockNumberParam::Height(EthU256::from(42u64)))
        );

        let error = GetBalance::try_parse_params(Some(Params::Array(vec![json!(encoded_address)])))
            .expect_err("block selector is required");
        assert_eq!(error.code(), ReservedErrorCode::InvalidParams as i64);

        let error =
            GetBalance::try_parse_params(Some(Params::Array(vec![json!("0x01"), json!("latest")])))
                .expect_err("address must contain exactly twenty bytes");
        assert_eq!(error.code(), ReservedErrorCode::InvalidParams as i64);
    }

    #[test]
    fn parses_eip_1898_hash_object_selector() {
        let encoded_address = format!("0x{}", "01".repeat(evm::ADDRESS_LENGTH));
        let block_hash = evm::Hash::new([0x2a; evm::HASH_LENGTH]);
        let params = GetBalance::try_parse_params(Some(Params::Array(vec![
            json!(encoded_address),
            json!({
                "blockHash": block_hash,
                "requireCanonical": false,
            }),
        ])))
        .expect("EIP-1898 block hash object should parse");

        assert_eq!(
            params.block,
            StateBlockParam::HashObject(BlockHashParam {
                block_hash,
                require_canonical: false,
            })
        );
    }

    #[tokio::test]
    async fn maps_supported_block_tags_to_casper_state_identifiers() {
        let client = BinaryPortMock::new();
        for tag in [
            BlockTag::Latest,
            BlockTag::Pending,
            BlockTag::Safe,
            BlockTag::Finalized,
        ] {
            let params = GetBalanceParams {
                address: EthAddress::from(address(10)),
                block: BlockNumberParam::Tag(tag).into(),
            };
            assert_eq!(
                params
                    .block
                    .resolve_state_identifier(&client, PendingPolicy::Latest)
                    .await
                    .unwrap(),
                None
            );
        }

        let earliest = GetBalanceParams {
            address: EthAddress::from(address(10)),
            block: BlockNumberParam::Tag(BlockTag::Earliest).into(),
        };
        assert_eq!(
            earliest
                .block
                .resolve_state_identifier(&client, PendingPolicy::Latest)
                .await
                .unwrap(),
            Some(GlobalStateIdentifier::BlockHeight(0))
        );
    }

    #[tokio::test]
    async fn pruned_historical_state_returns_no_such_state_root() {
        let client = Arc::new(BinaryPortMock::new());
        let evm_address = address(11);
        let block = Block::V2(
            TestBlockBuilder::new()
                .height(42)
                .build(&mut TestRng::new()),
        );
        client
            .add_block_header_req_res(
                block.clone_header(),
                InformationRequest::BlockHeader(Some(BlockIdentifier::Height(42))),
            )
            .await;
        client
            .when_then(
                state_request(
                    Some(GlobalStateIdentifier::BlockHeight(42)),
                    GlobalStateEntityQualifier::Item {
                        base_key: Key::Evm(EvmAddr::Account(evm_address)),
                        path: Vec::new(),
                    },
                ),
                BinaryResponse::new_error(BinaryPortErrorCode::RootNotFound),
            )
            .await;

        let error = GetBalance::do_handle_request(
            client.clone(),
            GetBalanceParams {
                address: evm_address.into(),
                block: BlockNumberParam::Height(EthU256::from(42u64)).into(),
            },
        )
        .await
        .expect_err("pruned balance state should fail");

        assert_eq!(error.code(), crate::rpcs::ErrorCode::NoSuchStateRoot as i64);
        client.verify_no_lingering().await;
    }

    #[test]
    fn rejects_balance_conversion_overflow() {
        let multiplication_error =
            balance_in_wei(U512::MAX, 2).expect_err("U512 multiplication should overflow");
        assert_eq!(
            multiplication_error.code(),
            ReservedErrorCode::InternalError as i64
        );

        let u256_error = balance_in_wei(U512::one() << 256, 1)
            .expect_err("Ethereum quantity should not exceed U256");
        assert_eq!(u256_error.code(), ReservedErrorCode::InternalError as i64);
    }

    #[tokio::test]
    async fn rejects_invalid_chainspec_data() {
        let client = BinaryPortMock::new();
        add_chainspec_response(
            &client,
            BinaryResponse::from_value(ChainspecRawBytes::new(
                b"not valid toml =".to_vec().into(),
                None,
                None,
            )),
        )
        .await;

        let error = read_evm_config(&client)
            .await
            .expect_err("invalid chainspec should fail");

        assert_eq!(error.code(), ReservedErrorCode::InternalError as i64);
        client.verify_no_lingering().await;
    }
}
