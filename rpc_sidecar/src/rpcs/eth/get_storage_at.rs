use std::{
    fmt,
    sync::{Arc, LazyLock},
};

use async_trait::async_trait;
use casper_json_rpc::{Error as RpcError, Params};
use casper_types::{EvmAddr, GlobalStateIdentifier, Key, StoredValue, U256, evm};
use schemars::{JsonSchema, r#gen::SchemaGenerator, schema::Schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as SerdeError};

use super::{
    super::{Error, NodeClient, RpcWithParams},
    types::{
        EthAddress, EthBytesMax32, PendingPolicy, StateBlockParam, internal_error,
        parse_positional_params,
    },
};
use crate::rpcs::docs::DocExample;

const DATA_32_BYTES: usize = 32;

static GET_STORAGE_AT_PARAMS_EXAMPLE: LazyLock<GetStorageAtParams> =
    LazyLock::new(|| GetStorageAtParams {
        address: EthAddress::from(evm::Address::ZERO),
        slot: EthBytesMax32::ZERO,
        block: StateBlockParam::default(),
    });
static DATA_32_EXAMPLE: LazyLock<EthData32> = LazyLock::new(EthData32::default);

/// Params for `eth_getStorageAt`.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct GetStorageAtParams {
    address: EthAddress,
    slot: EthBytesMax32,
    block: StateBlockParam,
}

impl DocExample for GetStorageAtParams {
    fn doc_example() -> &'static Self {
        &GET_STORAGE_AT_PARAMS_EXAMPLE
    }
}

#[derive(Deserialize)]
struct PositionalParams(EthAddress, EthBytesMax32, StateBlockParam);

impl From<PositionalParams> for GetStorageAtParams {
    fn from(params: PositionalParams) -> Self {
        GetStorageAtParams {
            address: params.0,
            slot: params.1,
            block: params.2,
        }
    }
}

/// Exactly 32 bytes of Ethereum JSON-RPC data encoded as `0x` hexadecimal.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub(crate) struct EthData32([u8; DATA_32_BYTES]);

impl EthData32 {
    const ZERO: Self = Self([0; DATA_32_BYTES]);

    fn from_u256(value: U256) -> Self {
        let mut bytes = [0; DATA_32_BYTES];
        value.to_big_endian(&mut bytes);
        EthData32(bytes)
    }

    fn to_hex(self) -> String {
        format!("0x{}", base16::encode_lower(&self.0))
    }
}

impl fmt::Display for EthData32 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_hex())
    }
}

impl Serialize for EthData32 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.collect_str(self)
        } else {
            self.0.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for EthData32 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let is_human_readable = deserializer.is_human_readable();
        if is_human_readable {
            let value = String::deserialize(deserializer)?;
            parse_data_32(&value)
                .map(EthData32)
                .map_err(D::Error::custom)
        } else {
            <[u8; DATA_32_BYTES]>::deserialize(deserializer).map(EthData32)
        }
    }
}

fn parse_data_32(value: &str) -> Result<[u8; DATA_32_BYTES], String> {
    let hex = value
        .strip_prefix("0x")
        .ok_or_else(|| "DATA32 value must start with 0x".to_string())?;
    if hex.len() != DATA_32_BYTES * 2 {
        return Err(format!(
            "DATA32 value must contain exactly {} hexadecimal digits",
            DATA_32_BYTES * 2
        ));
    }
    let bytes = base16::decode(hex.as_bytes()).map_err(|error| format!("invalid hex: {error}"))?;
    <[u8; DATA_32_BYTES]>::try_from(bytes.as_slice())
        .map_err(|_| "DATA32 value has an invalid length".to_string())
}

impl JsonSchema for EthData32 {
    fn schema_name() -> String {
        "EthData32".to_string()
    }

    fn json_schema(schema_generator: &mut SchemaGenerator) -> Schema {
        let schema = String::json_schema(schema_generator);
        let mut schema_object = schema.into_object();
        schema_object.metadata().description = Some(
            "Exactly 32 bytes of Ethereum JSON-RPC data encoded as 0x-prefixed hexadecimal."
                .to_string(),
        );
        let string_validation = schema_object.string();
        string_validation.min_length = Some((2 + DATA_32_BYTES * 2) as u32);
        string_validation.max_length = Some((2 + DATA_32_BYTES * 2) as u32);
        string_validation.pattern = Some(format!(r"^0x[0-9a-f]{{{}}}$", DATA_32_BYTES * 2));
        schema_object.into()
    }
}

impl DocExample for EthData32 {
    fn doc_example() -> &'static Self {
        &DATA_32_EXAMPLE
    }
}

async fn read_storage_at(
    node_client: &dyn NodeClient,
    state_identifier: Option<GlobalStateIdentifier>,
    address: evm::Address,
    slot: U256,
) -> Result<EthData32, RpcError> {
    let storage_key = Key::Evm(EvmAddr::Storage(evm::StorageAddr::new(address, slot)));
    let maybe_value = node_client
        .query_global_state(state_identifier, storage_key, vec![])
        .await
        .map_err(|error| Error::NodeRequest("EVM storage", error))?;

    match maybe_value.map(|value| value.into_inner().0) {
        Some(StoredValue::CLValue(cl_value)) => cl_value
            .into_t::<U256>()
            .map(EthData32::from_u256)
            .map_err(|error| {
                internal_error(format!(
                    "invalid EVM storage CLValue under {storage_key}: {error}"
                ))
            }),
        Some(other) => Err(internal_error(format!(
            "expected EVM storage CLValue under {storage_key}, found {}",
            other.type_name()
        ))),
        None => Ok(EthData32::ZERO),
    }
}

/// `eth_getStorageAt`.
pub struct GetStorageAt;

#[async_trait]
impl RpcWithParams for GetStorageAt {
    const METHOD: &'static str = "eth_getStorageAt";
    type RequestParams = GetStorageAtParams;
    type ResponseResult = EthData32;

    fn try_parse_params(maybe_params: Option<Params>) -> Result<Self::RequestParams, RpcError> {
        parse_positional_params::<PositionalParams>(maybe_params).map(Into::into)
    }

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: GetStorageAtParams,
    ) -> Result<EthData32, RpcError> {
        let state_identifier = params
            .block
            .resolve_state_identifier(node_client.as_ref(), PendingPolicy::Latest)
            .await?;
        read_storage_at(
            node_client.as_ref(),
            state_identifier,
            params.address.into_inner(),
            params.slot.value(),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use casper_binary_port::{
        BinaryResponse, Command, ErrorCode as BinaryPortErrorCode, GetRequest,
        GlobalStateEntityQualifier, GlobalStateQueryResult, GlobalStateRequest, InformationRequest,
    };
    use casper_json_rpc::ReservedErrorCode;
    use casper_types::{
        Block, BlockIdentifier, ByteCode, ByteCodeKind, CLValue, TestBlockBuilder, testing::TestRng,
    };
    use serde_json::json;

    use super::*;
    use crate::rpcs::{ErrorCode, eth::types::block_hash_to_evm_hash, test_utils::BinaryPortMock};

    const BLOCK_HEIGHT: u64 = 69;

    #[test]
    fn serializes_as_exactly_32_bytes_of_lowercase_hex() {
        let zero = serde_json::to_value(EthData32::ZERO).expect("zero should serialize");
        assert_eq!(zero, json!(format!("0x{}", "00".repeat(DATA_32_BYTES))));

        let one =
            serde_json::to_value(EthData32::from_u256(U256::one())).expect("one should serialize");
        assert_eq!(
            one,
            json!(format!("0x{}01", "00".repeat(DATA_32_BYTES - 1)))
        );

        let maximum = serde_json::to_value(EthData32::from_u256(U256::MAX))
            .expect("maximum should serialize");
        assert_eq!(maximum, json!(format!("0x{}", "ff".repeat(DATA_32_BYTES))));
    }

    #[test]
    fn data_32_deserialization_requires_the_exact_width() {
        let encoded = format!("\"0x{}\"", "ab".repeat(DATA_32_BYTES));
        let decoded: EthData32 = serde_json::from_str(&encoded).expect("DATA32 should parse");
        assert_eq!(decoded, EthData32([0xab; DATA_32_BYTES]));

        for invalid in ["\"0x0\"", "\"0x\"", "\"ab\"", "\"0xgg\""] {
            serde_json::from_str::<EthData32>(invalid)
                .expect_err("non-DATA32 response should fail to deserialize");
        }
    }

    #[test]
    fn data_32_schema_enforces_the_wire_format() {
        let mut generator = SchemaGenerator::default();
        let schema = EthData32::json_schema(&mut generator).into_object();
        let validation = schema.string.expect("DATA32 should have string validation");

        assert_eq!(validation.min_length, Some(66));
        assert_eq!(validation.max_length, Some(66));
        assert_eq!(validation.pattern.as_deref(), Some(r"^0x[0-9a-f]{64}$"));
    }

    #[test]
    fn parses_compact_and_padded_storage_slots() {
        let address = format!("0x{}", "11".repeat(evm::ADDRESS_LENGTH));
        for (slot, expected) in [
            ("0x0".to_string(), U256::zero()),
            ("0x1".to_string(), U256::one()),
            (format!("0x{}1", "0".repeat(63)), U256::one()),
            (format!("0x{}", "ff".repeat(DATA_32_BYTES)), U256::MAX),
        ] {
            let parsed = GetStorageAt::try_parse_params(Some(Params::Array(vec![
                json!(address),
                json!(slot),
                json!("latest"),
            ])))
            .expect("storage slot should parse");
            assert_eq!(parsed.slot.value(), expected);
        }
    }

    #[test]
    fn rejects_oversized_or_malformed_storage_slots() {
        let address = format!("0x{}", "11".repeat(evm::ADDRESS_LENGTH));
        for slot in [
            format!("0x1{}", "0".repeat(64)),
            "0x".to_string(),
            "0xgg".to_string(),
            "1".to_string(),
        ] {
            GetStorageAt::try_parse_params(Some(Params::Array(vec![
                json!(address),
                json!(slot),
                json!("latest"),
            ])))
            .expect_err("invalid storage slot should not parse");
        }
    }

    #[test]
    fn requires_an_exactly_20_byte_address_and_a_block_selector() {
        for address in [
            format!("0x{}", "11".repeat(evm::ADDRESS_LENGTH - 1)),
            format!("0x{}", "11".repeat(evm::ADDRESS_LENGTH + 1)),
        ] {
            GetStorageAt::try_parse_params(Some(Params::Array(vec![
                json!(address),
                json!("0x0"),
                json!("latest"),
            ])))
            .expect_err("non-address-width data should not parse");
        }

        let address = format!("0x{}", "11".repeat(evm::ADDRESS_LENGTH));
        GetStorageAt::try_parse_params(Some(Params::Array(vec![json!(address), json!("0x0")])))
            .expect_err("the block selector is required");
    }

    #[tokio::test]
    async fn reads_storage_value_through_the_rpc_handler() {
        let client = Arc::new(BinaryPortMock::new());
        let address = evm::Address::new([1; evm::ADDRESS_LENGTH]);
        let slot = U256::from(7u64);
        let value = U256::from_big_endian(&[0xab, 0xcd]);
        add_state_response(
            &client,
            None,
            storage_key(address, slot),
            BinaryResponse::from_option(Some(GlobalStateQueryResult::new(
                StoredValue::CLValue(CLValue::from_t(value).unwrap()),
                Vec::new(),
            ))),
        )
        .await;

        let params = GetStorageAt::try_parse_params(Some(Params::Array(vec![
            json!(String::from(EthAddress::from(address))),
            json!("0x7"),
            json!("latest"),
        ])))
        .expect("params should parse");
        let result = GetStorageAt::do_handle_request(client.clone(), params)
            .await
            .expect("storage lookup should succeed");

        assert_eq!(result, EthData32::from_u256(value));
        client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn missing_storage_key_returns_zero_word() {
        let client = BinaryPortMock::new();
        let address = evm::Address::new([2; evm::ADDRESS_LENGTH]);
        let slot = U256::from(3u64);
        add_state_response(
            &client,
            None,
            storage_key(address, slot),
            BinaryResponse::from_option::<GlobalStateQueryResult>(None),
        )
        .await;

        let result = read_storage_at(&client, None, address, slot)
            .await
            .expect("missing storage should read as zero");

        assert_eq!(result, EthData32::ZERO);
        client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn explicitly_stored_zero_returns_zero_word() {
        let client = BinaryPortMock::new();
        let address = evm::Address::new([12; evm::ADDRESS_LENGTH]);
        let slot = U256::from(13u64);
        add_state_response(
            &client,
            None,
            storage_key(address, slot),
            BinaryResponse::from_option(Some(GlobalStateQueryResult::new(
                StoredValue::CLValue(CLValue::from_t(U256::zero()).unwrap()),
                Vec::new(),
            ))),
        )
        .await;

        let result = read_storage_at(&client, None, address, slot)
            .await
            .expect("stored zero should read as zero");

        assert_eq!(result, EthData32::ZERO);
        client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn reads_storage_at_historical_height() {
        let rng = &mut TestRng::new();
        let block = Block::V2(TestBlockBuilder::new().height(BLOCK_HEIGHT).build(rng));
        let client = Arc::new(BinaryPortMock::new());
        let address = evm::Address::new([3; evm::ADDRESS_LENGTH]);
        let slot = U256::from(4u64);
        let value = U256::from(10u64);
        client
            .add_block_header_req_res(
                block.take_header(),
                InformationRequest::BlockHeader(Some(BlockIdentifier::Height(BLOCK_HEIGHT))),
            )
            .await;
        add_state_response(
            &client,
            Some(GlobalStateIdentifier::BlockHeight(BLOCK_HEIGHT)),
            storage_key(address, slot),
            BinaryResponse::from_option(Some(GlobalStateQueryResult::new(
                StoredValue::CLValue(CLValue::from_t(value).unwrap()),
                Vec::new(),
            ))),
        )
        .await;

        let params = GetStorageAt::try_parse_params(Some(Params::Array(vec![
            json!(String::from(EthAddress::from(address))),
            json!("0x4"),
            json!("0x45"),
        ])))
        .expect("height params should parse");
        assert_eq!(
            GetStorageAt::do_handle_request(client.clone(), params)
                .await
                .expect("historical storage lookup should succeed"),
            EthData32::from_u256(value)
        );
        client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn reads_storage_at_historical_hash() {
        let rng = &mut TestRng::new();
        let block = Block::V2(TestBlockBuilder::new().height(BLOCK_HEIGHT).build(rng));
        let block_hash = *block.hash();
        let client = Arc::new(BinaryPortMock::new());
        let address = evm::Address::new([5; evm::ADDRESS_LENGTH]);
        let slot = U256::from(6u64);
        let value = U256::from(11u64);
        client
            .add_block_header_req_res(
                block.take_header(),
                InformationRequest::BlockHeader(Some(BlockIdentifier::Hash(block_hash))),
            )
            .await;
        add_state_response(
            &client,
            Some(GlobalStateIdentifier::BlockHash(block_hash)),
            storage_key(address, slot),
            BinaryResponse::from_option(Some(GlobalStateQueryResult::new(
                StoredValue::CLValue(CLValue::from_t(value).unwrap()),
                Vec::new(),
            ))),
        )
        .await;

        let params = GetStorageAt::try_parse_params(Some(Params::Array(vec![
            json!(String::from(EthAddress::from(address))),
            json!("0x6"),
            json!(block_hash_to_evm_hash(block_hash)),
        ])))
        .expect("hash params should parse");
        assert_eq!(
            GetStorageAt::do_handle_request(client.clone(), params)
                .await
                .expect("historical storage lookup should succeed"),
            EthData32::from_u256(value)
        );
        client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn malformed_cl_value_is_an_internal_error() {
        let client = BinaryPortMock::new();
        let address = evm::Address::new([6; evm::ADDRESS_LENGTH]);
        let slot = U256::from(7u64);
        add_state_response(
            &client,
            None,
            storage_key(address, slot),
            BinaryResponse::from_option(Some(GlobalStateQueryResult::new(
                StoredValue::CLValue(CLValue::from_t(42u64).unwrap()),
                Vec::new(),
            ))),
        )
        .await;

        let error = read_storage_at(&client, None, address, slot)
            .await
            .expect_err("wrong CLValue type should fail");

        assert_eq!(error.code(), ReservedErrorCode::InternalError as i64);
        client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn wrong_stored_value_variant_is_an_internal_error() {
        let client = BinaryPortMock::new();
        let address = evm::Address::new([8; evm::ADDRESS_LENGTH]);
        let slot = U256::from(9u64);
        add_state_response(
            &client,
            None,
            storage_key(address, slot),
            BinaryResponse::from_option(Some(GlobalStateQueryResult::new(
                StoredValue::ByteCode(ByteCode::new(ByteCodeKind::EvmPrague, vec![0x00])),
                Vec::new(),
            ))),
        )
        .await;

        let error = read_storage_at(&client, None, address, slot)
            .await
            .expect_err("wrong stored value should fail");

        assert_eq!(error.code(), ReservedErrorCode::InternalError as i64);
        client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn pruned_historical_state_is_not_treated_as_zero() {
        let client = BinaryPortMock::new();
        let address = evm::Address::new([10; evm::ADDRESS_LENGTH]);
        let slot = U256::from(11u64);
        let identifier = Some(GlobalStateIdentifier::BlockHeight(BLOCK_HEIGHT));
        add_state_response(
            &client,
            identifier,
            storage_key(address, slot),
            BinaryResponse::new_error(BinaryPortErrorCode::RootNotFound),
        )
        .await;

        let error = read_storage_at(&client, identifier, address, slot)
            .await
            .expect_err("missing historical state root should fail");

        assert_eq!(error.code(), ErrorCode::NoSuchStateRoot as i64);
        client.verify_no_lingering().await;
    }

    fn storage_key(address: evm::Address, slot: U256) -> Key {
        Key::Evm(EvmAddr::Storage(evm::StorageAddr::new(address, slot)))
    }

    async fn add_state_response(
        client: &BinaryPortMock,
        state_identifier: Option<GlobalStateIdentifier>,
        key: Key,
        response: BinaryResponse,
    ) {
        let request = GlobalStateRequest::new(
            state_identifier,
            GlobalStateEntityQualifier::Item {
                base_key: key,
                path: Vec::new(),
            },
        );
        client
            .when_then(Command::Get(GetRequest::State(Box::new(request))), response)
            .await;
    }
}
