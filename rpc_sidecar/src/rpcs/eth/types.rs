use std::sync::LazyLock;

use casper_json_rpc::{Error as RpcError, ErrorCodeT, Params, ReservedErrorCode};
use casper_types::{BlockHash, BlockIdentifier, Digest, GlobalStateIdentifier, U256, evm};
use schemars::{JsonSchema, r#gen::SchemaGenerator, schema::Schema};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use super::eth_u256::EthU256;
use crate::{
    NodeClient,
    rpcs::{ErrorCode, common::get_block_header, docs::DocExample},
};

pub(super) const DEFAULT_ETH_CALL_GAS_LIMIT: u64 = 30_000_000;

static ETH_U256_EXAMPLE: LazyLock<EthU256> = LazyLock::new(|| EthU256::ZERO);
static ETH_ADDRESS_EXAMPLE: LazyLock<EthAddress> = LazyLock::new(|| EthAddress(evm::Address::ZERO));
static EVM_HASH_EXAMPLE: LazyLock<evm::Hash> = LazyLock::new(|| evm::Hash::ZERO);
static HEX_DATA_EXAMPLE: LazyLock<HexData> = LazyLock::new(|| HexData(Vec::new()));

impl DocExample for EthU256 {
    fn doc_example() -> &'static Self {
        &ETH_U256_EXAMPLE
    }
}

static OPTIONAL_ETH_U256_EXAMPLE: LazyLock<Option<EthU256>> = LazyLock::new(|| None);

/// Shared doc example for the `Option<EthU256>` quantity-or-null responses returned by the
/// block/uncle transaction-count RPCs (`null` when the referenced block is unknown).
impl DocExample for Option<EthU256> {
    fn doc_example() -> &'static Self {
        &OPTIONAL_ETH_U256_EXAMPLE
    }
}

/// Ethereum address encoded as fixed-width `0x` hexadecimal.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub(crate) struct EthAddress(evm::Address);

impl EthAddress {
    pub(super) fn into_inner(self) -> evm::Address {
        self.0
    }
}

impl From<evm::Address> for EthAddress {
    fn from(address: evm::Address) -> Self {
        EthAddress(address)
    }
}

impl From<EthAddress> for String {
    fn from(value: EthAddress) -> Self {
        format!("0x{}", value.0.to_hex_string())
    }
}

impl TryFrom<String> for EthAddress {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        parse_address(&value).map(EthAddress)
    }
}

impl JsonSchema for EthAddress {
    fn schema_name() -> String {
        "EthAddress".to_string()
    }

    fn json_schema(schema_generator: &mut SchemaGenerator) -> Schema {
        String::json_schema(schema_generator)
    }
}

impl DocExample for EthAddress {
    fn doc_example() -> &'static Self {
        &ETH_ADDRESS_EXAMPLE
    }
}

impl DocExample for evm::Hash {
    fn doc_example() -> &'static Self {
        &EVM_HASH_EXAMPLE
    }
}

/// Variable-width byte data encoded as `0x` hexadecimal.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub(crate) struct HexData(Vec<u8>);

impl HexData {
    pub(super) fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl From<Vec<u8>> for HexData {
    fn from(bytes: Vec<u8>) -> Self {
        HexData(bytes)
    }
}

impl From<&[u8]> for HexData {
    fn from(bytes: &[u8]) -> Self {
        HexData(bytes.to_vec())
    }
}

impl From<HexData> for String {
    fn from(value: HexData) -> Self {
        bytes_hex(&value.0)
    }
}

impl TryFrom<String> for HexData {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        parse_hex_bytes(&value).map(HexData)
    }
}

impl JsonSchema for HexData {
    fn schema_name() -> String {
        "EthHexData".to_string()
    }

    fn json_schema(schema_generator: &mut SchemaGenerator) -> Schema {
        String::json_schema(schema_generator)
    }
}

impl DocExample for HexData {
    fn doc_example() -> &'static Self {
        &HEX_DATA_EXAMPLE
    }
}

/// An Ethereum `bytesMax32` value: a 256-bit word encoded as `0x`-prefixed hexadecimal.
///
/// Compact and left-padded forms are both accepted — a caller may pass `0x1` for the word `1` —
/// and the value is serialized back as a canonical Ethereum quantity. Used wherever the
/// Execution API accepts a `bytesMax32`, e.g. the `eth_getStorageAt` slot and the
/// `eth_getProof` storage keys.
#[derive(
    Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize,
)]
#[serde(transparent)]
pub(crate) struct EthBytesMax32(EthU256);

impl EthBytesMax32 {
    /// The zero word.
    pub(super) const ZERO: Self = EthBytesMax32(EthU256::ZERO);

    /// Returns the word as a 256-bit integer.
    pub(super) fn value(self) -> U256 {
        self.0.value()
    }
}

impl JsonSchema for EthBytesMax32 {
    fn schema_name() -> String {
        "EthBytesMax32".to_string()
    }

    fn json_schema(schema_generator: &mut SchemaGenerator) -> Schema {
        let schema = String::json_schema(schema_generator);
        let mut schema_object = schema.into_object();
        schema_object.metadata().description = Some(
            "An Ethereum bytesMax32 value encoded as 0x-prefixed hexadecimal with 1 to 64 \
             digits; compact and left-padded forms are accepted."
                .to_string(),
        );
        let string_validation = schema_object.string();
        string_validation.min_length = Some(3);
        string_validation.max_length = Some(2 + evm::HASH_LENGTH as u32 * 2);
        string_validation.pattern = Some(r"^0x[0-9a-fA-F]{1,64}$".to_string());
        schema_object.into()
    }
}

/// Ethereum block tags supported by this first-pass RPC surface.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub(crate) enum BlockTag {
    /// Genesis block.
    Earliest,
    /// Latest finalized Casper state known to the node.
    Latest,
    /// Alias for latest until pending EVM state is exposed.
    Pending,
    /// Alias for latest until safe EVM state is exposed.
    Safe,
    /// Alias for latest finalized Casper state.
    Finalized,
}

impl Default for BlockTag {
    fn default() -> Self {
        BlockTag::Latest
    }
}

/// Ethereum block selector accepting either a named tag or a numeric height.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub(crate) enum BlockNumberParam {
    /// Latest block known by the node.
    Tag(BlockTag),
    /// Concrete block height.
    Height(EthU256),
}

impl Default for BlockNumberParam {
    fn default() -> Self {
        BlockNumberParam::Tag(BlockTag::Latest)
    }
}

impl BlockNumberParam {
    pub(super) fn identifier(self) -> Result<Option<BlockIdentifier>, RpcError> {
        match self {
            BlockNumberParam::Tag(BlockTag::Earliest) => Ok(Some(BlockIdentifier::Height(0))),
            BlockNumberParam::Tag(
                BlockTag::Latest | BlockTag::Pending | BlockTag::Safe | BlockTag::Finalized,
            ) => Ok(None),
            BlockNumberParam::Height(height) => height
                .as_u64()
                .map(|height| Some(BlockIdentifier::Height(height)))
                .map_err(invalid_params),
        }
    }

    pub(super) fn height(self) -> Result<Option<u64>, RpcError> {
        match self {
            BlockNumberParam::Tag(BlockTag::Earliest) => Ok(Some(0)),
            BlockNumberParam::Tag(
                BlockTag::Latest | BlockTag::Pending | BlockTag::Safe | BlockTag::Finalized,
            ) => Ok(None),
            BlockNumberParam::Height(height) => height.as_u64().map(Some).map_err(invalid_params),
        }
    }
}

/// How a JSON-RPC method handles the `pending` block tag.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum PendingPolicy {
    /// Resolve pending state to the latest complete Casper state.
    Latest,
    /// Reject pending state because the method requires a complete block.
    Reject,
}

/// EIP-1898 block hash object.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct BlockHashParam {
    pub(super) block_hash: evm::Hash,
    #[serde(default)]
    pub(super) require_canonical: bool,
}

/// EIP-1898 block number object.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct BlockNumberObjectParam {
    pub(super) block_number: EthU256,
}

/// Ethereum state selector supporting EIP-1898 and raw block hashes.
///
/// Hash forms precede quantity forms so a 32-byte hexadecimal string is interpreted as a block
/// hash, matching the released Ethereum Execution API schema.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub(crate) enum StateBlockParam {
    /// An EIP-1898 block hash object.
    HashObject(BlockHashParam),
    /// An EIP-1898 block number object.
    NumberObject(BlockNumberObjectParam),
    /// A raw 32-byte block hash.
    Hash(evm::Hash),
    /// A named tag or concrete block height.
    Number(BlockNumberParam),
}

impl Default for StateBlockParam {
    fn default() -> Self {
        StateBlockParam::Number(BlockNumberParam::default())
    }
}

impl From<BlockNumberParam> for StateBlockParam {
    fn from(value: BlockNumberParam) -> Self {
        StateBlockParam::Number(value)
    }
}

impl StateBlockParam {
    /// Resolves this selector and validates every explicit height or hash against node storage.
    pub(super) async fn resolve_block_identifier(
        &self,
        node_client: &dyn NodeClient,
        pending_policy: PendingPolicy,
    ) -> Result<Option<BlockIdentifier>, RpcError> {
        match *self {
            StateBlockParam::Number(BlockNumberParam::Tag(BlockTag::Pending)) => {
                match pending_policy {
                    PendingPolicy::Latest => Ok(None),
                    PendingPolicy::Reject => {
                        Err(invalid_params("eth_call does not support pending state"))
                    }
                }
            }
            StateBlockParam::Number(BlockNumberParam::Tag(BlockTag::Earliest)) => {
                // TODO: this needs to be handled correctly, reuse the mechanism of fetching
                // block range used in eth_syncing
                Ok(Some(BlockIdentifier::Height(0)))
            }
            StateBlockParam::Number(BlockNumberParam::Tag(
                BlockTag::Latest | BlockTag::Safe | BlockTag::Finalized,
            )) => Ok(None),
            StateBlockParam::Number(BlockNumberParam::Height(height))
            | StateBlockParam::NumberObject(BlockNumberObjectParam {
                block_number: height,
            }) => {
                let identifier = BlockIdentifier::Height(height.as_u64().map_err(invalid_params)?);
                get_block_header(node_client, Some(identifier)).await?;
                Ok(Some(identifier))
            }
            StateBlockParam::Hash(block_hash) => {
                resolve_block_hash(node_client, block_hash, false).await
            }
            StateBlockParam::HashObject(BlockHashParam {
                block_hash,
                require_canonical,
            }) => resolve_block_hash(node_client, block_hash, require_canonical).await,
        }
    }

    /// Resolves this selector for a global-state query.
    pub(super) async fn resolve_state_identifier(
        &self,
        node_client: &dyn NodeClient,
        pending_policy: PendingPolicy,
    ) -> Result<Option<GlobalStateIdentifier>, RpcError> {
        self.resolve_block_identifier(node_client, pending_policy)
            .await
            .map(|identifier| identifier.map(GlobalStateIdentifier::from))
    }
}

async fn resolve_block_hash(
    node_client: &dyn NodeClient,
    block_hash: evm::Hash,
    require_canonical: bool,
) -> Result<Option<BlockIdentifier>, RpcError> {
    let identifier = BlockIdentifier::Hash(BlockHash::new(Digest::from_raw(block_hash.value())));
    // Resolve the requested hash first so block-not-found takes precedence over canonicality.
    let selected_header = get_block_header(node_client, Some(identifier)).await?;

    if require_canonical {
        let canonical_header = get_block_header(
            node_client,
            Some(BlockIdentifier::Height(selected_header.height())),
        )
        .await?;
        if canonical_header.block_hash() != selected_header.block_hash() {
            return Err(RpcError::new(
                ErrorCode::InvalidBlock,
                "requested block is not canonical",
            ));
        }
    }

    Ok(Some(identifier))
}

pub(super) fn parse_positional_params<T>(maybe_params: Option<Params>) -> Result<T, RpcError>
where
    T: DeserializeOwned,
{
    let params = match maybe_params {
        Some(params) => Value::from(params),
        None => {
            return Err(RpcError::new(
                ReservedErrorCode::InvalidParams,
                "Missing 'params' field",
            ));
        }
    };
    serde_json::from_value(params).map_err(|error| {
        RpcError::new(
            ReservedErrorCode::InvalidParams,
            format!("Failed to parse 'params' field: {error}"),
        )
    })
}

pub(super) fn block_hash_to_evm_hash(block_hash: impl AsRef<[u8]>) -> evm::Hash {
    let mut bytes = [0u8; evm::HASH_LENGTH];
    bytes.copy_from_slice(block_hash.as_ref());
    evm::Hash::new(bytes)
}

fn parse_address(value: &str) -> Result<evm::Address, String> {
    let bytes = parse_fixed_hex::<{ evm::ADDRESS_LENGTH }>(value)?;
    Ok(evm::Address::new(bytes))
}

fn parse_fixed_hex<const N: usize>(value: &str) -> Result<[u8; N], String> {
    let bytes = parse_hex_bytes(value)?;
    <[u8; N]>::try_from(bytes.as_slice())
        .map_err(|_| format!("expected {N} bytes, got {}", bytes.len()))
}

fn parse_hex_bytes(value: &str) -> Result<Vec<u8>, String> {
    let hex = value
        .strip_prefix("0x")
        .ok_or_else(|| "hex value must start with 0x".to_string())?;
    if hex.len() % 2 != 0 {
        return Err("hex value must have an even number of digits".to_string());
    }
    (0..hex.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&hex[index..index + 2], 16)
                .map_err(|error| format!("invalid hex: {error}"))
        })
        .collect()
}

pub(super) fn invalid_params(message: impl Into<String>) -> RpcError {
    RpcError::new(ReservedErrorCode::InvalidParams, message.into())
}

pub(super) fn internal_error(message: impl ToString) -> RpcError {
    RpcError::new(ReservedErrorCode::InternalError, message.to_string())
}

/// JSON-RPC error code for an `eth_*` method that this server's API recognizes but does not
/// implement.
///
/// EIP-1474's non-standard error-code table assigns "method not supported" to `-32004`. This is
/// a distinct concept from [`ReservedErrorCode::MethodNotFound`] (`-32601`), which means the
/// method is not part of the API at all. `-32004` is outside the JSON-RPC pre-defined range
/// (`-32768..=-32100`), so a non-reserved code type carrying it is passed through verbatim by
/// [`RpcError::new`], message text included.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Deserialize)]
#[repr(i64)]
pub(super) enum EthApiErrorCode {
    /// Method not supported.
    MethodNotSupported = -32004,
}

impl From<EthApiErrorCode> for (i64, &'static str) {
    fn from(error_code: EthApiErrorCode) -> Self {
        match error_code {
            EthApiErrorCode::MethodNotSupported => (error_code as i64, "method not supported"),
        }
    }
}

impl ErrorCodeT for EthApiErrorCode {}

/// Returns an error indicating that a method recognized by this server's API is not supported by
/// this implementation (as opposed to [`ReservedErrorCode::MethodNotFound`], which signals that
/// the method is not part of the API at all). The caller-supplied `message` is carried as the
/// error's `data`; the wire code/message are EIP-1474's `-32004` / "method not supported".
pub(super) fn method_not_supported(message: impl Into<String>) -> RpcError {
    RpcError::new(EthApiErrorCode::MethodNotSupported, message.into())
}

fn bytes_hex(bytes: &[u8]) -> String {
    format!("0x{}", base16::encode_lower(bytes))
}

#[cfg(test)]
mod tests {
    use casper_binary_port::{BinaryResponse, Command, InformationRequest};
    use casper_types::{
        AvailableBlockRange, Block, BlockHeader, TestBlockBuilder, testing::TestRng,
    };
    use serde_json::json;

    use super::*;
    use crate::rpcs::test_utils::BinaryPortMock;

    #[test]
    fn evm_hash_uses_json_rpc_hex_prefix() {
        let hash = evm::Hash::new([0xab; evm::HASH_LENGTH]);
        let expected_hex = "ab".repeat(evm::HASH_LENGTH);

        assert_eq!(format!("{hash}"), format!("0x{expected_hex}"));

        let encoded = serde_json::to_string(&hash).expect("evm hash should serialize");
        assert_eq!(encoded, format!("\"0x{expected_hex}\""));
        assert_eq!(
            serde_json::from_str::<evm::Hash>(&encoded).expect("evm hash should parse"),
            hash
        );
    }

    #[test]
    fn state_block_param_accepts_all_supported_wire_shapes() {
        let hash = evm::Hash::new([0xab; evm::HASH_LENGTH]);
        let hash_hex = format!("0x{}", hash.to_hex_string());

        let cases = [
            (
                json!("latest"),
                StateBlockParam::Number(BlockNumberParam::Tag(BlockTag::Latest)),
            ),
            (
                json!("0x1234"),
                StateBlockParam::Number(BlockNumberParam::Height(EthU256::from(0x1234u64))),
            ),
            (json!(hash_hex.clone()), StateBlockParam::Hash(hash)),
            (
                json!({ "blockNumber": "0x1234" }),
                StateBlockParam::NumberObject(BlockNumberObjectParam {
                    block_number: EthU256::from(0x1234u64),
                }),
            ),
            (
                json!({ "blockHash": hash_hex.clone() }),
                StateBlockParam::HashObject(BlockHashParam {
                    block_hash: hash,
                    require_canonical: false,
                }),
            ),
            (
                json!({ "blockHash": hash_hex.clone(), "requireCanonical": false }),
                StateBlockParam::HashObject(BlockHashParam {
                    block_hash: hash,
                    require_canonical: false,
                }),
            ),
            (
                json!({ "blockHash": hash_hex, "requireCanonical": true }),
                StateBlockParam::HashObject(BlockHashParam {
                    block_hash: hash,
                    require_canonical: true,
                }),
            ),
        ];

        for (value, expected) in cases {
            assert_eq!(
                serde_json::from_value::<StateBlockParam>(value).unwrap(),
                expected
            );
        }

        for tag in ["earliest", "pending", "safe", "finalized"] {
            serde_json::from_value::<StateBlockParam>(json!(tag))
                .expect("supported block tag should parse");
        }
    }

    #[test]
    fn state_block_param_rejects_invalid_eip_1898_objects() {
        let hash = format!("0x{}", "ab".repeat(evm::HASH_LENGTH));
        for value in [
            json!({}),
            json!({ "requireCanonical": true }),
            json!({ "blockHash": hash, "blockNumber": "0x1" }),
            json!({ "blockNumber": "0x1", "requireCanonical": true }),
            json!({ "blockHash": hash, "unknown": true }),
            json!({ "blockNumber": "0x1", "unknown": true }),
            json!({ "blockHash": hash, "requireCanonical": "true" }),
            json!({ "blockHash": hash, "requireCanonical": null }),
            json!({ "blockHash": "0x1234" }),
            json!({ "blockHash": format!("0x{}", "gg".repeat(evm::HASH_LENGTH)) }),
            json!({ "blockNumber": "latest" }),
            json!({ "blockNumber": format!("0x1{}", "0".repeat(64)) }),
        ] {
            serde_json::from_value::<StateBlockParam>(value)
                .expect_err("invalid EIP-1898 object should be rejected");
        }
    }

    #[tokio::test]
    async fn state_block_param_preserves_tag_policy() {
        let client = BinaryPortMock::new();
        for tag in [BlockTag::Latest, BlockTag::Safe, BlockTag::Finalized] {
            assert_eq!(
                StateBlockParam::Number(BlockNumberParam::Tag(tag))
                    .resolve_state_identifier(&client, PendingPolicy::Latest)
                    .await
                    .unwrap(),
                None
            );
        }
        assert_eq!(
            StateBlockParam::Number(BlockNumberParam::Tag(BlockTag::Pending))
                .resolve_state_identifier(&client, PendingPolicy::Latest)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            StateBlockParam::Number(BlockNumberParam::Tag(BlockTag::Earliest))
                .resolve_state_identifier(&client, PendingPolicy::Latest)
                .await
                .unwrap(),
            Some(GlobalStateIdentifier::BlockHeight(0))
        );

        let error = StateBlockParam::Number(BlockNumberParam::Tag(BlockTag::Pending))
            .resolve_block_identifier(&client, PendingPolicy::Reject)
            .await
            .expect_err("pending must be rejected for eth_call");
        assert_eq!(error.code(), ReservedErrorCode::InvalidParams as i64);
        client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn canonical_block_number_object_is_validated_before_being_resolved() {
        let rng = &mut TestRng::new();
        let block = Block::V2(TestBlockBuilder::new().height(42).build(rng));
        let client = BinaryPortMock::new();
        client
            .add_block_header_req_res(
                block.clone_header(),
                InformationRequest::BlockHeader(Some(BlockIdentifier::Height(42))),
            )
            .await;

        let result = StateBlockParam::NumberObject(BlockNumberObjectParam {
            block_number: EthU256::from(42u64),
        })
        .resolve_state_identifier(&client, PendingPolicy::Latest)
        .await
        .unwrap();

        assert_eq!(result, Some(GlobalStateIdentifier::BlockHeight(42)));
        client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn missing_block_number_object_returns_no_such_block() {
        let client = BinaryPortMock::new();
        let header_request = InformationRequest::BlockHeader(Some(BlockIdentifier::Height(42)))
            .try_into()
            .unwrap();
        client
            .when_then(
                Command::Get(header_request),
                BinaryResponse::from_option(None::<BlockHeader>),
            )
            .await;
        let range_request = InformationRequest::AvailableBlockRange.try_into().unwrap();
        client
            .when_then(
                Command::Get(range_request),
                BinaryResponse::from_value(AvailableBlockRange::new(10, 20)),
            )
            .await;

        let error = StateBlockParam::NumberObject(BlockNumberObjectParam {
            block_number: EthU256::from(42u64),
        })
        .resolve_block_identifier(&client, PendingPolicy::Latest)
        .await
        .expect_err("missing block number must fail validation");

        assert_eq!(error.code(), ErrorCode::NoSuchBlock as i64);
        client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn hash_forms_resolve_known_noncanonical_block_without_canonical_check() {
        let rng = &mut TestRng::new();
        let block = Block::V2(TestBlockBuilder::new().height(42).build(rng));
        let block_hash = *block.hash();
        for selector in [
            StateBlockParam::Hash(block_hash_to_evm_hash(block_hash)),
            StateBlockParam::HashObject(BlockHashParam {
                block_hash: block_hash_to_evm_hash(block_hash),
                require_canonical: false,
            }),
        ] {
            let client = BinaryPortMock::new();
            client
                .add_block_header_req_res(
                    block.clone_header(),
                    InformationRequest::BlockHeader(Some(BlockIdentifier::Hash(block_hash))),
                )
                .await;

            let result = selector
                .resolve_state_identifier(&client, PendingPolicy::Latest)
                .await
                .unwrap();

            assert_eq!(result, Some(GlobalStateIdentifier::BlockHash(block_hash)));
            client.verify_no_lingering().await;
        }
    }

    #[tokio::test]
    async fn require_canonical_accepts_canonical_hash() {
        let rng = &mut TestRng::new();
        let block = Block::V2(TestBlockBuilder::new().height(42).build(rng));
        let block_hash = *block.hash();
        let client = BinaryPortMock::new();
        client
            .add_block_header_req_res(
                block.clone_header(),
                InformationRequest::BlockHeader(Some(BlockIdentifier::Hash(block_hash))),
            )
            .await;
        client
            .add_block_header_req_res(
                block.clone_header(),
                InformationRequest::BlockHeader(Some(BlockIdentifier::Height(42))),
            )
            .await;

        let result = StateBlockParam::HashObject(BlockHashParam {
            block_hash: block_hash_to_evm_hash(block_hash),
            require_canonical: true,
        })
        .resolve_block_identifier(&client, PendingPolicy::Latest)
        .await
        .unwrap();

        assert_eq!(result, Some(BlockIdentifier::Hash(block_hash)));
        client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn require_canonical_rejects_noncanonical_hash() {
        let rng = &mut TestRng::new();
        let selected = Block::V2(TestBlockBuilder::new().height(42).build(rng));
        let canonical = Block::V2(TestBlockBuilder::new().height(42).build(rng));
        assert_ne!(selected.hash(), canonical.hash());
        let selected_hash = *selected.hash();
        let client = BinaryPortMock::new();
        client
            .add_block_header_req_res(
                selected.clone_header(),
                InformationRequest::BlockHeader(Some(BlockIdentifier::Hash(selected_hash))),
            )
            .await;
        client
            .add_block_header_req_res(
                canonical.clone_header(),
                InformationRequest::BlockHeader(Some(BlockIdentifier::Height(42))),
            )
            .await;

        let error = StateBlockParam::HashObject(BlockHashParam {
            block_hash: block_hash_to_evm_hash(selected_hash),
            require_canonical: true,
        })
        .resolve_block_identifier(&client, PendingPolicy::Latest)
        .await
        .expect_err("noncanonical block must be rejected");

        assert_eq!(
            error,
            RpcError::new(ErrorCode::InvalidBlock, "requested block is not canonical")
        );
        client.verify_no_lingering().await;
    }

    #[tokio::test]
    async fn missing_hash_returns_no_such_block_before_canonicality_check() {
        let block_hash = BlockHash::new(Digest::from_raw([0x44; 32]));
        let client = BinaryPortMock::new();
        let header_request =
            InformationRequest::BlockHeader(Some(BlockIdentifier::Hash(block_hash)))
                .try_into()
                .unwrap();
        client
            .when_then(
                Command::Get(header_request),
                BinaryResponse::from_option(None::<BlockHeader>),
            )
            .await;
        let range_request = InformationRequest::AvailableBlockRange.try_into().unwrap();
        client
            .when_then(
                Command::Get(range_request),
                BinaryResponse::from_value(AvailableBlockRange::new(10, 20)),
            )
            .await;

        let error = StateBlockParam::HashObject(BlockHashParam {
            block_hash: block_hash_to_evm_hash(block_hash),
            require_canonical: true,
        })
        .resolve_block_identifier(&client, PendingPolicy::Latest)
        .await
        .expect_err("missing block must fail before canonicality lookup");

        assert_eq!(error.code(), ErrorCode::NoSuchBlock as i64);
        client.verify_no_lingering().await;
    }
}
