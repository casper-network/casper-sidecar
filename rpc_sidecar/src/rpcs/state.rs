//! RPCs related to the state.

use std::{collections::BTreeMap, str, sync::Arc};

use async_trait::async_trait;
use once_cell::sync::Lazy;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    common::{self, EntityOrAccount, MERKLE_PROOF},
    docs::{DocExample, DOCS_EXAMPLE_API_VERSION},
    ApiVersion, Error, NodeClient, RpcError, RpcWithOptionalParams, RpcWithParams,
    CURRENT_API_VERSION,
};
use casper_binary_port::DictionaryItemIdentifier;
use casper_binary_port::PurseIdentifier as PortPurseIdentifier;
#[cfg(test)]
use casper_types::testing::TestRng;
use casper_types::{
    account::{Account, AccountHash},
    addressable_entity::EntityKindTag,
    bytesrepr::Bytes,
    system::{
        auction::{
            BidKind, EraValidators, SeigniorageRecipientsSnapshot, ValidatorWeights,
            SEIGNIORAGE_RECIPIENTS_SNAPSHOT_KEY,
        },
        AUCTION,
    },
    AddressableEntity, AddressableEntityHash, AuctionState, BlockHash, BlockHeader, BlockHeaderV2,
    BlockIdentifier, BlockTime, BlockV2, CLValue, Digest, EntityAddr, EntryPoint, EntryPointValue,
    GlobalStateIdentifier, Key, KeyTag, PublicKey, SecretKey, StoredValue, URef, U512,
};
#[cfg(test)]
use rand::Rng;

static GET_ITEM_PARAMS: Lazy<GetItemParams> = Lazy::new(|| GetItemParams {
    state_root_hash: *BlockHeaderV2::example().state_root_hash(),
    key: Key::from_formatted_str(
        "deploy-af684263911154d26fa05be9963171802801a0b6aff8f199b7391eacb8edc9e1",
    )
    .unwrap(),
    path: vec!["inner".to_string()],
});
static GET_ITEM_RESULT: Lazy<GetItemResult> = Lazy::new(|| GetItemResult {
    api_version: DOCS_EXAMPLE_API_VERSION,
    stored_value: StoredValue::CLValue(CLValue::from_t(1u64).unwrap()),
    merkle_proof: MERKLE_PROOF.clone(),
});
static GET_BALANCE_PARAMS: Lazy<GetBalanceParams> = Lazy::new(|| GetBalanceParams {
    state_root_hash: *BlockHeaderV2::example().state_root_hash(),
    purse_uref: "uref-09480c3248ef76b603d386f3f4f8a5f87f597d4eaffd475433f861af187ab5db-007"
        .to_string(),
});
static GET_BALANCE_RESULT: Lazy<GetBalanceResult> = Lazy::new(|| GetBalanceResult {
    api_version: DOCS_EXAMPLE_API_VERSION,
    balance_value: U512::from(123_456),
    merkle_proof: MERKLE_PROOF.clone(),
});
static GET_AUCTION_INFO_PARAMS: Lazy<GetAuctionInfoParams> = Lazy::new(|| GetAuctionInfoParams {
    block_identifier: BlockIdentifier::Hash(*BlockHash::example()),
});
static GET_AUCTION_INFO_RESULT: Lazy<GetAuctionInfoResult> = Lazy::new(|| GetAuctionInfoResult {
    api_version: DOCS_EXAMPLE_API_VERSION,
    auction_state: AuctionState::doc_example().clone(),
});
static GET_ACCOUNT_INFO_PARAMS: Lazy<GetAccountInfoParams> = Lazy::new(|| {
    let secret_key = SecretKey::ed25519_from_bytes([0; 32]).unwrap();
    let public_key = PublicKey::from(&secret_key);
    GetAccountInfoParams {
        account_identifier: AccountIdentifier::PublicKey(public_key),
        block_identifier: Some(BlockIdentifier::Hash(*BlockHash::example())),
    }
});
static GET_ACCOUNT_INFO_RESULT: Lazy<GetAccountInfoResult> = Lazy::new(|| GetAccountInfoResult {
    api_version: DOCS_EXAMPLE_API_VERSION,
    account: Account::doc_example().clone(),
    merkle_proof: MERKLE_PROOF.clone(),
});
static GET_ADDRESSABLE_ENTITY_PARAMS: Lazy<GetAddressableEntityParams> =
    Lazy::new(|| GetAddressableEntityParams {
        entity_identifier: EntityIdentifier::EntityAddr(EntityAddr::new_account([0; 32])),
        block_identifier: Some(BlockIdentifier::Hash(*BlockHash::example())),
    });
static GET_ADDRESSABLE_ENTITY_RESULT: Lazy<GetAddressableEntityResult> =
    Lazy::new(|| GetAddressableEntityResult {
        api_version: DOCS_EXAMPLE_API_VERSION,
        merkle_proof: MERKLE_PROOF.clone(),
        entity: EntityOrAccount::AddressableEntity {
            entity: AddressableEntity::example().clone(),
            named_keys: [("key".to_string(), Key::Hash([0u8; 32]))]
                .iter()
                .cloned()
                .collect::<BTreeMap<_, _>>()
                .into(),
            entry_points: vec![EntryPointValue::new_v1_entry_point_value(
                EntryPoint::default_with_name("entry_point"),
            )],
        },
    });
static GET_DICTIONARY_ITEM_PARAMS: Lazy<GetDictionaryItemParams> =
    Lazy::new(|| GetDictionaryItemParams {
        state_root_hash: *BlockHeaderV2::example().state_root_hash(),
        dictionary_identifier: DictionaryIdentifier::URef {
            seed_uref: "uref-09480c3248ef76b603d386f3f4f8a5f87f597d4eaffd475433f861af187ab5db-007"
                .to_string(),
            dictionary_item_key: "a_unique_entry_identifier".to_string(),
        },
    });
static GET_DICTIONARY_ITEM_RESULT: Lazy<GetDictionaryItemResult> =
    Lazy::new(|| GetDictionaryItemResult {
        api_version: DOCS_EXAMPLE_API_VERSION,
        dictionary_key:
            "dictionary-67518854aa916c97d4e53df8570c8217ccc259da2721b692102d76acd0ee8d1f"
                .to_string(),
        stored_value: StoredValue::CLValue(CLValue::from_t(1u64).unwrap()),
        merkle_proof: MERKLE_PROOF.clone(),
    });
static QUERY_GLOBAL_STATE_PARAMS: Lazy<QueryGlobalStateParams> =
    Lazy::new(|| QueryGlobalStateParams {
        state_identifier: Some(GlobalStateIdentifier::BlockHash(*BlockV2::example().hash())),
        key: Key::from_formatted_str(
            "deploy-af684263911154d26fa05be9963171802801a0b6aff8f199b7391eacb8edc9e1",
        )
        .unwrap(),
        path: vec![],
    });
static QUERY_GLOBAL_STATE_RESULT: Lazy<QueryGlobalStateResult> =
    Lazy::new(|| QueryGlobalStateResult {
        api_version: DOCS_EXAMPLE_API_VERSION,
        block_header: Some(BlockHeaderV2::example().clone().into()),
        stored_value: StoredValue::Account(Account::doc_example().clone()),
        merkle_proof: MERKLE_PROOF.clone(),
    });
static GET_TRIE_PARAMS: Lazy<GetTrieParams> = Lazy::new(|| GetTrieParams {
    trie_key: *BlockHeaderV2::example().state_root_hash(),
});
static GET_TRIE_RESULT: Lazy<GetTrieResult> = Lazy::new(|| GetTrieResult {
    api_version: DOCS_EXAMPLE_API_VERSION,
    maybe_trie_bytes: None,
});
static QUERY_BALANCE_PARAMS: Lazy<QueryBalanceParams> = Lazy::new(|| QueryBalanceParams {
    state_identifier: Some(GlobalStateIdentifier::BlockHash(*BlockHash::example())),
    purse_identifier: PurseIdentifier::MainPurseUnderAccountHash(AccountHash::new([9u8; 32])),
});
static QUERY_BALANCE_RESULT: Lazy<QueryBalanceResult> = Lazy::new(|| QueryBalanceResult {
    api_version: DOCS_EXAMPLE_API_VERSION,
    balance: U512::from(123_456),
});
static QUERY_BALANCE_DETAILS_PARAMS: Lazy<QueryBalanceDetailsParams> =
    Lazy::new(|| QueryBalanceDetailsParams {
        state_identifier: Some(GlobalStateIdentifier::BlockHash(*BlockHash::example())),
        purse_identifier: PurseIdentifier::MainPurseUnderAccountHash(AccountHash::new([9u8; 32])),
    });
static QUERY_BALANCE_DETAILS_RESULT: Lazy<QueryBalanceDetailsResult> =
    Lazy::new(|| QueryBalanceDetailsResult {
        api_version: DOCS_EXAMPLE_API_VERSION,
        total_balance: U512::from(123_456),
        available_balance: U512::from(123_456),
        total_balance_proof: MERKLE_PROOF.clone(),
        holds: vec![BalanceHoldWithProof {
            time: BlockTime::new(0),
            amount: U512::from(123_456),
            proof: MERKLE_PROOF.clone(),
        }],
    });

/// Params for "state_get_item" RPC request.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetItemParams {
    /// Hash of the state root.
    pub state_root_hash: Digest,
    /// The key under which to query.
    pub key: Key,
    /// The path components starting from the key as base.
    #[serde(default)]
    pub path: Vec<String>,
}

impl DocExample for GetItemParams {
    fn doc_example() -> &'static Self {
        &GET_ITEM_PARAMS
    }
}

/// Result for "state_get_item" RPC response.
#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetItemResult {
    /// The RPC API version.
    #[schemars(with = "String")]
    pub api_version: ApiVersion,
    /// The stored value.
    pub stored_value: StoredValue,
    /// The Merkle proof.
    pub merkle_proof: String,
}

impl DocExample for GetItemResult {
    fn doc_example() -> &'static Self {
        &GET_ITEM_RESULT
    }
}

/// "state_get_item" RPC.
pub struct GetItem {}

#[async_trait]
impl RpcWithParams for GetItem {
    const METHOD: &'static str = "state_get_item";
    type RequestParams = GetItemParams;
    type ResponseResult = GetItemResult;

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: Self::RequestParams,
    ) -> Result<Self::ResponseResult, RpcError> {
        let state_identifier = GlobalStateIdentifier::StateRootHash(params.state_root_hash);
        let (stored_value, merkle_proof) = node_client
            .query_global_state(Some(state_identifier), params.key, params.path)
            .await
            .map_err(|err| Error::NodeRequest("global state item", err))?
            .ok_or(Error::GlobalStateEntryNotFound)?
            .into_inner();

        Ok(Self::ResponseResult {
            api_version: CURRENT_API_VERSION,
            stored_value,
            merkle_proof: common::encode_proof(&merkle_proof)?,
        })
    }
}

/// Params for "state_get_balance" RPC request.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetBalanceParams {
    /// The hash of state root.
    pub state_root_hash: Digest,
    /// Formatted URef.
    pub purse_uref: String,
}

impl DocExample for GetBalanceParams {
    fn doc_example() -> &'static Self {
        &GET_BALANCE_PARAMS
    }
}

/// Result for "state_get_balance" RPC response.
#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetBalanceResult {
    /// The RPC API version.
    #[schemars(with = "String")]
    pub api_version: ApiVersion,
    /// The available balance in motes (total balance - sum of all active holds).
    pub balance_value: U512,
    /// The Merkle proof.
    pub merkle_proof: String,
}

impl DocExample for GetBalanceResult {
    fn doc_example() -> &'static Self {
        &GET_BALANCE_RESULT
    }
}

/// "state_get_balance" RPC.
pub struct GetBalance {}

#[async_trait]
impl RpcWithParams for GetBalance {
    const METHOD: &'static str = "state_get_balance";
    type RequestParams = GetBalanceParams;
    type ResponseResult = GetBalanceResult;

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: Self::RequestParams,
    ) -> Result<Self::ResponseResult, RpcError> {
        let purse_uref =
            URef::from_formatted_str(&params.purse_uref).map_err(Error::InvalidPurseURef)?;

        let state_id = GlobalStateIdentifier::StateRootHash(params.state_root_hash);
        let purse_id = PortPurseIdentifier::Purse(purse_uref);
        let balance = node_client
            .read_balance(Some(state_id), purse_id)
            .await
            .map_err(|err| Error::NodeRequest("balance", err))?;

        Ok(Self::ResponseResult {
            api_version: CURRENT_API_VERSION,
            balance_value: balance.available_balance,
            merkle_proof: common::encode_proof(&vec![*balance.total_balance_proof])?,
        })
    }
}

/// Params for "state_get_auction_info" RPC request.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetAuctionInfoParams {
    /// The block identifier.
    pub block_identifier: BlockIdentifier,
}

impl DocExample for GetAuctionInfoParams {
    fn doc_example() -> &'static Self {
        &GET_AUCTION_INFO_PARAMS
    }
}

/// Result for "state_get_auction_info" RPC response.
#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetAuctionInfoResult {
    /// The RPC API version.
    #[schemars(with = "String")]
    pub api_version: ApiVersion,
    /// The auction state.
    pub auction_state: AuctionState,
}

impl DocExample for GetAuctionInfoResult {
    fn doc_example() -> &'static Self {
        &GET_AUCTION_INFO_RESULT
    }
}

/// "state_get_auction_info" RPC.
pub struct GetAuctionInfo {}

#[async_trait]
impl RpcWithOptionalParams for GetAuctionInfo {
    const METHOD: &'static str = "state_get_auction_info";
    type OptionalRequestParams = GetAuctionInfoParams;
    type ResponseResult = GetAuctionInfoResult;

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        maybe_params: Option<Self::OptionalRequestParams>,
    ) -> Result<Self::ResponseResult, RpcError> {
        let block_identifier = maybe_params.map(|params| params.block_identifier);
        let block_header = common::get_block_header(&*node_client, block_identifier).await?;

        let state_identifier = block_identifier.map(GlobalStateIdentifier::from);
        let legacy_bid_stored_values = node_client
            .query_global_state_by_tag(state_identifier, KeyTag::Bid)
            .await
            .map_err(|err| Error::NodeRequest("auction bids", err))?
            .into_iter()
            .map(|value| {
                Ok(BidKind::Unified(
                    value.into_bid().ok_or(Error::InvalidAuctionState)?.into(),
                ))
            });
        let bid_stored_values = node_client
            .query_global_state_by_tag(state_identifier, KeyTag::BidAddr)
            .await
            .map_err(|err| Error::NodeRequest("auction bids", err))?
            .into_iter()
            .map(|value| value.into_bid_kind().ok_or(Error::InvalidAuctionState));
        let bids = legacy_bid_stored_values
            .chain(bid_stored_values)
            .collect::<Result<Vec<_>, Error>>()?;

        // always retrieve the latest system contract registry, old versions of the node
        // did not write it to the global state
        let (registry_value, _) = node_client
            .query_global_state(None, Key::SystemEntityRegistry, vec![])
            .await
            .map_err(|err| Error::NodeRequest("system contract registry", err))?
            .ok_or(Error::GlobalStateEntryNotFound)?
            .into_inner();
        let registry: BTreeMap<String, AddressableEntityHash> = registry_value
            .into_cl_value()
            .ok_or(Error::InvalidAuctionState)?
            .into_t()
            .map_err(|_| Error::InvalidAuctionState)?;

        let &auction_hash = registry.get(AUCTION).ok_or(Error::InvalidAuctionState)?;
        let (snapshot_value, _) = if let Some(result) = node_client
            .query_global_state(
                state_identifier,
                Key::addressable_entity_key(EntityKindTag::System, auction_hash),
                vec![SEIGNIORAGE_RECIPIENTS_SNAPSHOT_KEY.to_owned()],
            )
            .await
            .map_err(|err| Error::NodeRequest("auction snapshot", err))?
        {
            result.into_inner()
        } else {
            node_client
                .query_global_state(
                    state_identifier,
                    Key::Hash(auction_hash.value()),
                    vec![SEIGNIORAGE_RECIPIENTS_SNAPSHOT_KEY.to_owned()],
                )
                .await
                .map_err(|err| Error::NodeRequest("auction snapshot", err))?
                .ok_or(Error::GlobalStateEntryNotFound)?
                .into_inner()
        };
        let snapshot = snapshot_value
            .into_cl_value()
            .ok_or(Error::InvalidAuctionState)?
            .into_t()
            .map_err(|_| Error::InvalidAuctionState)?;

        let validators = era_validators_from_snapshot(snapshot);
        let auction_state = AuctionState::new(
            *block_header.state_root_hash(),
            block_header.height(),
            validators,
            bids,
        );

        Ok(Self::ResponseResult {
            api_version: CURRENT_API_VERSION,
            auction_state,
        })
    }
}

/// Identifier of an account.
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
#[serde(deny_unknown_fields, untagged)]
pub enum AccountIdentifier {
    /// The public key of an account
    PublicKey(PublicKey),
    /// The account hash of an account
    AccountHash(AccountHash),
}

impl AccountIdentifier {
    #[cfg(test)]
    pub fn random(rng: &mut TestRng) -> Self {
        match rng.gen_range(0..2) {
            0 => AccountIdentifier::PublicKey(PublicKey::random(rng)),
            1 => AccountIdentifier::AccountHash(rng.gen()),
            _ => unreachable!(),
        }
    }
}

/// Params for "state_get_account_info" RPC request
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetAccountInfoParams {
    /// The public key of the Account.
    #[serde(alias = "public_key")]
    pub account_identifier: AccountIdentifier,
    /// The block identifier.
    pub block_identifier: Option<BlockIdentifier>,
}

impl DocExample for GetAccountInfoParams {
    fn doc_example() -> &'static Self {
        &GET_ACCOUNT_INFO_PARAMS
    }
}

/// Result for "state_get_account_info" RPC response.
#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetAccountInfoResult {
    /// The RPC API version.
    #[schemars(with = "String")]
    pub api_version: ApiVersion,
    /// The account.
    pub account: Account,
    /// The Merkle proof.
    pub merkle_proof: String,
}

impl DocExample for GetAccountInfoResult {
    fn doc_example() -> &'static Self {
        &GET_ACCOUNT_INFO_RESULT
    }
}

/// "state_get_account_info" RPC.
pub struct GetAccountInfo {}

#[async_trait]
impl RpcWithParams for GetAccountInfo {
    const METHOD: &'static str = "state_get_account_info";
    type RequestParams = GetAccountInfoParams;
    type ResponseResult = GetAccountInfoResult;

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: Self::RequestParams,
    ) -> Result<Self::ResponseResult, RpcError> {
        let maybe_state_identifier = params.block_identifier.map(GlobalStateIdentifier::from);
        let base_key = {
            let account_hash = match params.account_identifier {
                AccountIdentifier::PublicKey(public_key) => public_key.to_account_hash(),
                AccountIdentifier::AccountHash(account_hash) => account_hash,
            };
            Key::Account(account_hash)
        };
        let (account_value, merkle_proof) = node_client
            .query_global_state(maybe_state_identifier, base_key, vec![])
            .await
            .map_err(|err| Error::NodeRequest("account info", err))?
            .ok_or(Error::AccountNotFound)?
            .into_inner();
        let account = account_value
            .into_account()
            .ok_or(Error::AccountMigratedToEntity)?;

        Ok(Self::ResponseResult {
            api_version: CURRENT_API_VERSION,
            account,
            merkle_proof: common::encode_proof(&merkle_proof)?,
        })
    }
}

/// Identifier of an addressable entity.
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
#[serde(deny_unknown_fields)]
pub enum EntityIdentifier {
    /// The public key of an account.
    PublicKey(PublicKey),
    /// The account hash of an account.
    AccountHash(AccountHash),
    /// The address of an addressable entity.
    EntityAddr(EntityAddr),
}

impl EntityIdentifier {
    #[cfg(test)]
    pub fn random(rng: &mut TestRng) -> Self {
        match rng.gen_range(0..3) {
            0 => EntityIdentifier::PublicKey(PublicKey::random(rng)),
            1 => EntityIdentifier::AccountHash(rng.gen()),
            2 => EntityIdentifier::EntityAddr(rng.gen()),
            _ => unreachable!(),
        }
    }
}

/// Params for "state_get_entity" RPC request
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetAddressableEntityParams {
    /// The identifier of the entity.
    pub entity_identifier: EntityIdentifier,
    /// The block identifier.
    pub block_identifier: Option<BlockIdentifier>,
}

impl DocExample for GetAddressableEntityParams {
    fn doc_example() -> &'static Self {
        &GET_ADDRESSABLE_ENTITY_PARAMS
    }
}

/// Result for "state_get_entity" RPC response.
#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetAddressableEntityResult {
    /// The RPC API version.
    #[schemars(with = "String")]
    pub api_version: ApiVersion,
    /// The addressable entity or a legacy account.
    pub entity: EntityOrAccount,
    /// The Merkle proof.
    pub merkle_proof: String,
}

impl DocExample for GetAddressableEntityResult {
    fn doc_example() -> &'static Self {
        &GET_ADDRESSABLE_ENTITY_RESULT
    }
}

/// "state_get_entity" RPC.
pub struct GetAddressableEntity {}

#[async_trait]
impl RpcWithParams for GetAddressableEntity {
    const METHOD: &'static str = "state_get_entity";
    type RequestParams = GetAddressableEntityParams;
    type ResponseResult = GetAddressableEntityResult;

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: Self::RequestParams,
    ) -> Result<Self::ResponseResult, RpcError> {
        let state_identifier = params.block_identifier.map(GlobalStateIdentifier::from);
        let (entity, merkle_proof) = match params.entity_identifier {
            EntityIdentifier::EntityAddr(addr) => {
                let result = common::resolve_entity_addr(&*node_client, addr, state_identifier)
                    .await?
                    .ok_or(Error::AddressableEntityNotFound)?;
                let named_keys =
                    common::get_entity_named_keys(&*node_client, addr, state_identifier).await?;
                let entry_points =
                    common::get_entity_entry_points(&*node_client, addr, state_identifier).await?;
                (
                    EntityOrAccount::AddressableEntity {
                        entity: result.value,
                        named_keys,
                        entry_points,
                    },
                    result.merkle_proof,
                )
            }
            EntityIdentifier::PublicKey(public_key) => {
                let account_hash = public_key.to_account_hash();
                common::resolve_account_hash(&*node_client, account_hash, state_identifier)
                    .await?
                    .ok_or(Error::AddressableEntityNotFound)?
                    .into_inner()
            }
            EntityIdentifier::AccountHash(account_hash) => {
                common::resolve_account_hash(&*node_client, account_hash, state_identifier)
                    .await?
                    .ok_or(Error::AddressableEntityNotFound)?
                    .into_inner()
            }
        };
        Ok(Self::ResponseResult {
            api_version: CURRENT_API_VERSION,
            entity,
            merkle_proof: common::encode_proof(&merkle_proof)?,
        })
    }
}

#[derive(Serialize, Deserialize, Debug, JsonSchema, Clone)]
/// Options for dictionary item lookups.
pub enum DictionaryIdentifier {
    /// Lookup a dictionary item via an Account's named keys.
    AccountNamedKey {
        /// The account key as a formatted string whose named keys contains dictionary_name.
        key: String,
        /// The named key under which the dictionary seed URef is stored.
        dictionary_name: String,
        /// The dictionary item key formatted as a string.
        dictionary_item_key: String,
    },
    /// Lookup a dictionary item via a Contract's named keys.
    ContractNamedKey {
        /// The contract key as a formatted string whose named keys contains dictionary_name.
        key: String,
        /// The named key under which the dictionary seed URef is stored.
        dictionary_name: String,
        /// The dictionary item key formatted as a string.
        dictionary_item_key: String,
    },
    /// Lookup a dictionary item via an entities named keys.
    EntityNamedKey {
        /// The entity address formatted as a string.
        key: String,
        /// The named key under which the dictionary seed URef is stored.
        dictionary_name: String,
        /// The dictionary item key formatted as a string.
        dictionary_item_key: String,
    },
    /// Lookup a dictionary item via its seed URef.
    URef {
        /// The dictionary's seed URef.
        seed_uref: String,
        /// The dictionary item key formatted as a string.
        dictionary_item_key: String,
    },
    /// Lookup a dictionary item via its unique key.
    Dictionary(String),
}

/// Params for "state_get_dictionary_item" RPC request.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetDictionaryItemParams {
    /// Hash of the state root
    pub state_root_hash: Digest,
    /// The Dictionary query identifier.
    pub dictionary_identifier: DictionaryIdentifier,
}

impl DocExample for GetDictionaryItemParams {
    fn doc_example() -> &'static Self {
        &GET_DICTIONARY_ITEM_PARAMS
    }
}

/// Result for "state_get_dictionary_item" RPC response.
#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetDictionaryItemResult {
    /// The RPC API version.
    #[schemars(with = "String")]
    pub api_version: ApiVersion,
    /// The key under which the value is stored.
    pub dictionary_key: String,
    /// The stored value.
    pub stored_value: StoredValue,
    /// The Merkle proof.
    pub merkle_proof: String,
}

impl DocExample for GetDictionaryItemResult {
    fn doc_example() -> &'static Self {
        &GET_DICTIONARY_ITEM_RESULT
    }
}

/// "state_get_dictionary_item" RPC.
pub struct GetDictionaryItem {}

#[async_trait]
impl RpcWithParams for GetDictionaryItem {
    const METHOD: &'static str = "state_get_dictionary_item";
    type RequestParams = GetDictionaryItemParams;
    type ResponseResult = GetDictionaryItemResult;

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: Self::RequestParams,
    ) -> Result<Self::ResponseResult, RpcError> {
        let state_identifier = GlobalStateIdentifier::StateRootHash(params.state_root_hash);

        let dictionary_key = match params.dictionary_identifier {
            DictionaryIdentifier::AccountNamedKey {
                key,
                dictionary_name,
                dictionary_item_key,
            } => {
                let hash = AccountHash::from_formatted_str(&key)
                    .map_err(|err| Error::InvalidDictionaryKey(err.to_string()))?;
                DictionaryItemIdentifier::AccountNamedKey {
                    hash,
                    dictionary_name,
                    dictionary_item_key,
                }
            }
            DictionaryIdentifier::ContractNamedKey {
                key,
                dictionary_name,
                dictionary_item_key,
            } => {
                let hash = Key::from_formatted_str(&key)
                    .map_err(|err| Error::InvalidDictionaryKey(err.to_string()))?
                    .into_hash_addr()
                    .ok_or_else(|| Error::InvalidDictionaryKey("not a hash address".to_owned()))?;
                DictionaryItemIdentifier::ContractNamedKey {
                    hash,
                    dictionary_name,
                    dictionary_item_key,
                }
            }
            DictionaryIdentifier::EntityNamedKey {
                key,
                dictionary_name,
                dictionary_item_key,
            } => {
                let addr = EntityAddr::from_formatted_str(&key)
                    .map_err(|err| Error::InvalidDictionaryKey(err.to_string()))?;
                DictionaryItemIdentifier::EntityNamedKey {
                    addr,
                    dictionary_name,
                    dictionary_item_key,
                }
            }
            DictionaryIdentifier::URef {
                seed_uref,
                dictionary_item_key,
            } => {
                let seed_uref = URef::from_formatted_str(&seed_uref)
                    .map_err(|err| Error::InvalidDictionaryKey(err.to_string()))?;
                DictionaryItemIdentifier::URef {
                    seed_uref,
                    dictionary_item_key,
                }
            }
            DictionaryIdentifier::Dictionary(dictionary_item_key) => {
                let key = Key::from_formatted_str(&dictionary_item_key)
                    .map_err(|err| Error::InvalidDictionaryKey(err.to_string()))?;
                let dict_key = key.as_dictionary().ok_or_else(|| {
                    Error::InvalidDictionaryKey("not a dictionary key".to_owned())
                })?;
                DictionaryItemIdentifier::DictionaryItem(*dict_key)
            }
        };
        let (key, result) = node_client
            .query_dictionary_item(Some(state_identifier), dictionary_key)
            .await
            .map_err(|err| Error::NodeRequest("dictionary item", err))?
            .ok_or(Error::GlobalStateEntryNotFound)?
            .into_inner();
        let (stored_value, merkle_proof) = result.into_inner();

        Ok(Self::ResponseResult {
            api_version: CURRENT_API_VERSION,
            dictionary_key: key.to_formatted_string(),
            stored_value,
            merkle_proof: common::encode_proof(&merkle_proof)?,
        })
    }
}

/// Params for "query_global_state" RPC
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QueryGlobalStateParams {
    /// The identifier used for the query. If not provided, the tip of the chain will be used.
    pub state_identifier: Option<GlobalStateIdentifier>,
    /// The key under which to query.
    pub key: Key,
    /// The path components starting from the key as base.
    #[serde(default)]
    pub path: Vec<String>,
}

impl DocExample for QueryGlobalStateParams {
    fn doc_example() -> &'static Self {
        &QUERY_GLOBAL_STATE_PARAMS
    }
}

/// Result for "query_global_state" RPC response.
#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QueryGlobalStateResult {
    /// The RPC API version.
    #[schemars(with = "String")]
    pub api_version: ApiVersion,
    /// The block header if a Block hash was provided.
    pub block_header: Option<BlockHeader>,
    /// The stored value.
    pub stored_value: StoredValue,
    /// The Merkle proof.
    pub merkle_proof: String,
}

impl DocExample for QueryGlobalStateResult {
    fn doc_example() -> &'static Self {
        &QUERY_GLOBAL_STATE_RESULT
    }
}

/// "query_global_state" RPC
pub struct QueryGlobalState {}

#[async_trait]
impl RpcWithParams for QueryGlobalState {
    const METHOD: &'static str = "query_global_state";
    type RequestParams = QueryGlobalStateParams;
    type ResponseResult = QueryGlobalStateResult;

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: Self::RequestParams,
    ) -> Result<Self::ResponseResult, RpcError> {
        let block_header = match params.state_identifier {
            Some(GlobalStateIdentifier::BlockHash(block_hash)) => {
                let identifier = BlockIdentifier::Hash(block_hash);
                Some(common::get_block_header(&*node_client, Some(identifier)).await?)
            }
            Some(GlobalStateIdentifier::BlockHeight(block_height)) => {
                let identifier = BlockIdentifier::Height(block_height);
                Some(common::get_block_header(&*node_client, Some(identifier)).await?)
            }
            _ => None,
        };

        let (stored_value, merkle_proof) = node_client
            .query_global_state(params.state_identifier, params.key, params.path)
            .await
            .map_err(|err| Error::NodeRequest("global state item", err))?
            .ok_or(Error::GlobalStateEntryNotFound)?
            .into_inner();

        Ok(Self::ResponseResult {
            api_version: CURRENT_API_VERSION,
            block_header,
            stored_value,
            merkle_proof: common::encode_proof(&merkle_proof)?,
        })
    }
}

/// Identifier of a purse.
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub enum PurseIdentifier {
    /// The main purse of the account identified by this public key.
    MainPurseUnderPublicKey(PublicKey),
    /// The main purse of the account identified by this account hash.
    MainPurseUnderAccountHash(AccountHash),
    /// The main purse of the account identified by this entity address.
    MainPurseUnderEntityAddr(EntityAddr),
    /// The purse identified by this URef.
    PurseUref(URef),
}

impl PurseIdentifier {
    pub fn into_port_purse_identifier(self) -> PortPurseIdentifier {
        match self {
            Self::MainPurseUnderPublicKey(public_key) => PortPurseIdentifier::PublicKey(public_key),
            Self::MainPurseUnderAccountHash(account_hash) => {
                PortPurseIdentifier::Account(account_hash)
            }
            Self::MainPurseUnderEntityAddr(entity_addr) => PortPurseIdentifier::Entity(entity_addr),
            Self::PurseUref(uref) => PortPurseIdentifier::Purse(uref),
        }
    }
}

/// Params for "query_balance" RPC request.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct QueryBalanceParams {
    /// The state identifier used for the query, if none is passed
    /// the tip of the chain will be used.
    pub state_identifier: Option<GlobalStateIdentifier>,
    /// The identifier to obtain the purse corresponding to balance query.
    pub purse_identifier: PurseIdentifier,
}

impl DocExample for QueryBalanceParams {
    fn doc_example() -> &'static Self {
        &QUERY_BALANCE_PARAMS
    }
}

/// Result for "query_balance" RPC response.
#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, JsonSchema)]
pub struct QueryBalanceResult {
    /// The RPC API version.
    #[schemars(with = "String")]
    pub api_version: ApiVersion,
    /// The available balance in motes (total balance - sum of all active holds).
    pub balance: U512,
}

impl DocExample for QueryBalanceResult {
    fn doc_example() -> &'static Self {
        &QUERY_BALANCE_RESULT
    }
}

/// "query_balance" RPC.
pub struct QueryBalance {}

#[async_trait]
impl RpcWithParams for QueryBalance {
    const METHOD: &'static str = "query_balance";
    type RequestParams = QueryBalanceParams;
    type ResponseResult = QueryBalanceResult;

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: Self::RequestParams,
    ) -> Result<Self::ResponseResult, RpcError> {
        let purse_id = params.purse_identifier.into_port_purse_identifier();
        let balance = node_client
            .read_balance(params.state_identifier, purse_id)
            .await
            .map_err(|err| Error::NodeRequest("balance", err))?;
        Ok(Self::ResponseResult {
            api_version: CURRENT_API_VERSION,
            balance: balance.available_balance,
        })
    }
}

/// Params for "query_balance_details" RPC request.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct QueryBalanceDetailsParams {
    /// The identifier for the state used for the query, if none is passed,
    /// the latest block will be used.
    pub state_identifier: Option<GlobalStateIdentifier>,
    /// The identifier to obtain the purse corresponding to balance query.
    pub purse_identifier: PurseIdentifier,
}

impl DocExample for QueryBalanceDetailsParams {
    fn doc_example() -> &'static Self {
        &QUERY_BALANCE_DETAILS_PARAMS
    }
}

/// Result for "query_balance_details" RPC response.
#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, JsonSchema)]
pub struct QueryBalanceDetailsResult {
    /// The RPC API version.
    #[schemars(with = "String")]
    pub api_version: ApiVersion,
    /// The purses total balance, not considering holds.
    pub total_balance: U512,
    /// The available balance in motes (total balance - sum of all active holds).
    pub available_balance: U512,
    /// A proof that the given value is present in the Merkle trie.
    pub total_balance_proof: String,
    /// Holds active at the requested point in time.
    pub holds: Vec<BalanceHoldWithProof>,
}

impl DocExample for QueryBalanceDetailsResult {
    fn doc_example() -> &'static Self {
        &QUERY_BALANCE_DETAILS_RESULT
    }
}

#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, JsonSchema)]
pub struct BalanceHoldWithProof {
    /// The block time at which the hold was created.
    pub time: BlockTime,
    /// The amount in the hold.
    pub amount: U512,
    /// A proof that the given value is present in the Merkle trie.
    pub proof: String,
}

/// "query_balance_details" RPC.
pub struct QueryBalanceDetails {}

#[async_trait]
impl RpcWithParams for QueryBalanceDetails {
    const METHOD: &'static str = "query_balance_details";
    type RequestParams = QueryBalanceDetailsParams;
    type ResponseResult = QueryBalanceDetailsResult;

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: Self::RequestParams,
    ) -> Result<Self::ResponseResult, RpcError> {
        let purse_id = params.purse_identifier.into_port_purse_identifier();
        let balance = node_client
            .read_balance(params.state_identifier, purse_id)
            .await
            .map_err(|err| Error::NodeRequest("balance", err))?;

        let holds = balance
            .balance_holds
            .into_iter()
            .flat_map(|(time, holds)| {
                holds.into_iter().map(move |(_, (amount, proof))| {
                    Ok(BalanceHoldWithProof {
                        time,
                        amount,
                        proof: common::encode_proof(&vec![proof])?,
                    })
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;

        Ok(Self::ResponseResult {
            api_version: CURRENT_API_VERSION,
            total_balance: balance.total_balance,
            available_balance: balance.available_balance,
            total_balance_proof: common::encode_proof(&vec![*balance.total_balance_proof])?,
            holds,
        })
    }
}

/// Parameters for "state_get_trie" RPC request.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct GetTrieParams {
    /// A trie key.
    pub trie_key: Digest,
}

impl DocExample for GetTrieParams {
    fn doc_example() -> &'static Self {
        &GET_TRIE_PARAMS
    }
}

/// Result for "state_get_trie" RPC response.
#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetTrieResult {
    /// The RPC API version.
    #[schemars(with = "String")]
    pub api_version: ApiVersion,
    /// A list of keys read under the specified prefix.
    #[schemars(
        with = "Option<String>",
        description = "A trie from global state storage, bytesrepr serialized and hex-encoded."
    )]
    pub maybe_trie_bytes: Option<Bytes>,
}

impl DocExample for GetTrieResult {
    fn doc_example() -> &'static Self {
        &GET_TRIE_RESULT
    }
}

/// `state_get_trie` RPC.
pub struct GetTrie {}

#[async_trait]
impl RpcWithParams for GetTrie {
    const METHOD: &'static str = "state_get_trie";
    type RequestParams = GetTrieParams;
    type ResponseResult = GetTrieResult;

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: Self::RequestParams,
    ) -> Result<Self::ResponseResult, RpcError> {
        let maybe_trie = node_client
            .read_trie_bytes(params.trie_key)
            .await
            .map_err(|err| Error::NodeRequest("trie", err))?;

        Ok(Self::ResponseResult {
            api_version: CURRENT_API_VERSION,
            maybe_trie_bytes: maybe_trie.map(Into::into),
        })
    }
}

fn era_validators_from_snapshot(snapshot: SeigniorageRecipientsSnapshot) -> EraValidators {
    snapshot
        .into_iter()
        .map(|(era_id, recipients)| {
            let validator_weights = recipients
                .into_iter()
                .filter_map(|(public_key, bid)| bid.total_stake().map(|stake| (public_key, stake)))
                .collect::<ValidatorWeights>();
            (era_id, validator_weights)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        {
            convert::TryFrom,
            iter::{self, FromIterator},
        },
    };

    use crate::{rpcs::ErrorCode, ClientError, SUPPORTED_PROTOCOL_VERSION};
    use casper_binary_port::{
        BalanceResponse, BinaryRequest, BinaryResponse, BinaryResponseAndRequest,
        DictionaryQueryResult, GetRequest, GlobalStateQueryResult, GlobalStateRequest,
        InformationRequestTag, KeyPrefix,
    };
    use casper_types::{
        addressable_entity::{MessageTopics, NamedKeyValue, NamedKeys},
        global_state::{TrieMerkleProof, TrieMerkleProofStep},
        system::auction::{Bid, BidKind, ValidatorBid},
        testing::TestRng,
        AccessRights, AddressableEntity, AvailableBlockRange, Block, ByteCodeHash, EntityKind,
        PackageHash, ProtocolVersion, TestBlockBuilder, TransactionRuntime,
    };
    use pretty_assertions::assert_eq;
    use rand::Rng;

    use super::*;

    #[tokio::test]
    async fn should_read_state_item() {
        let rng = &mut TestRng::new();
        let key = rng.gen::<Key>();
        let stored_value = StoredValue::CLValue(CLValue::from_t(rng.gen::<i32>()).unwrap());
        let merkle_proof = vec![TrieMerkleProof::new(
            key,
            stored_value.clone(),
            VecDeque::from_iter([TrieMerkleProofStep::random(rng)]),
        )];
        let expected = GlobalStateQueryResult::new(stored_value.clone(), merkle_proof.clone());

        let resp = GetItem::do_handle_request(
            Arc::new(ValidGlobalStateResultMock(expected.clone())),
            GetItemParams {
                state_root_hash: rng.gen(),
                key,
                path: vec![],
            },
        )
        .await
        .expect("should handle request");

        assert_eq!(
            resp,
            GetItemResult {
                api_version: CURRENT_API_VERSION,
                stored_value,
                merkle_proof: common::encode_proof(&merkle_proof).expect("should encode proof"),
            }
        );
    }

    #[tokio::test]
    async fn should_read_balance() {
        let rng = &mut TestRng::new();
        let available_balance = rng.gen();
        let total_balance = rng.gen();
        let balance = BalanceResponse {
            total_balance,
            available_balance,
            total_balance_proof: Box::new(TrieMerkleProof::new(
                Key::Account(rng.gen()),
                StoredValue::CLValue(CLValue::from_t(rng.gen::<i32>()).unwrap()),
                VecDeque::from_iter([TrieMerkleProofStep::random(rng)]),
            )),
            balance_holds: BTreeMap::new(),
        };

        let resp = GetBalance::do_handle_request(
            Arc::new(ValidBalanceMock(balance.clone())),
            GetBalanceParams {
                state_root_hash: rng.gen(),
                purse_uref: URef::new(rng.gen(), AccessRights::empty()).to_formatted_string(),
            },
        )
        .await
        .expect("should handle request");

        assert_eq!(
            resp,
            GetBalanceResult {
                api_version: CURRENT_API_VERSION,
                balance_value: available_balance,
                merkle_proof: common::encode_proof(&vec![*balance.total_balance_proof])
                    .expect("should encode proof"),
            }
        );
    }

    #[tokio::test]
    async fn should_read_auction_info() {
        struct ClientMock {
            block: Block,
            bids: Vec<BidKind>,
            legacy_bids: Vec<Bid>,
            contract_hash: AddressableEntityHash,
            snapshot: SeigniorageRecipientsSnapshot,
        }

        #[async_trait]
        impl NodeClient for ClientMock {
            async fn send_request(
                &self,
                req: BinaryRequest,
            ) -> Result<BinaryResponseAndRequest, ClientError> {
                match req {
                    BinaryRequest::Get(GetRequest::Information { info_type_tag, .. })
                        if InformationRequestTag::try_from(info_type_tag)
                            == Ok(InformationRequestTag::BlockHeader) =>
                    {
                        Ok(BinaryResponseAndRequest::new(
                            BinaryResponse::from_value(
                                self.block.clone_header(),
                                SUPPORTED_PROTOCOL_VERSION,
                            ),
                            &[],
                        ))
                    }
                    BinaryRequest::Get(GetRequest::State(req))
                        if matches!(
                            &*req,
                            GlobalStateRequest::AllItems {
                                key_tag: KeyTag::Bid,
                                ..
                            }
                        ) =>
                    {
                        let bids = self
                            .legacy_bids
                            .iter()
                            .cloned()
                            .map(|bid| StoredValue::Bid(bid.into()))
                            .collect::<Vec<_>>();
                        Ok(BinaryResponseAndRequest::new(
                            BinaryResponse::from_value(bids, SUPPORTED_PROTOCOL_VERSION),
                            &[],
                        ))
                    }
                    BinaryRequest::Get(GetRequest::State(req))
                        if matches!(
                            &*req,
                            GlobalStateRequest::AllItems {
                                key_tag: KeyTag::BidAddr,
                                ..
                            }
                        ) =>
                    {
                        let bids = self
                            .bids
                            .iter()
                            .cloned()
                            .map(StoredValue::BidKind)
                            .collect::<Vec<_>>();
                        Ok(BinaryResponseAndRequest::new(
                            BinaryResponse::from_value(bids, SUPPORTED_PROTOCOL_VERSION),
                            &[],
                        ))
                    }
                    BinaryRequest::Get(GetRequest::State(req))
                        if matches!(
                            &*req,
                            GlobalStateRequest::Item {
                                base_key: Key::SystemEntityRegistry,
                                ..
                            }
                        ) =>
                    {
                        let system_contracts =
                            iter::once((AUCTION.to_string(), self.contract_hash))
                                .collect::<BTreeMap<_, _>>();
                        let result = GlobalStateQueryResult::new(
                            StoredValue::CLValue(CLValue::from_t(system_contracts).unwrap()),
                            vec![],
                        );
                        Ok(BinaryResponseAndRequest::new(
                            BinaryResponse::from_value(result, SUPPORTED_PROTOCOL_VERSION),
                            &[],
                        ))
                    }
                    BinaryRequest::Get(GetRequest::State(req))
                        if matches!(
                            &*req,
                            GlobalStateRequest::Item {
                                base_key: Key::AddressableEntity(_),
                                ..
                            }
                        ) =>
                    {
                        let result = GlobalStateQueryResult::new(
                            StoredValue::CLValue(CLValue::from_t(self.snapshot.clone()).unwrap()),
                            vec![],
                        );
                        Ok(BinaryResponseAndRequest::new(
                            BinaryResponse::from_value(result, SUPPORTED_PROTOCOL_VERSION),
                            &[],
                        ))
                    }
                    req => unimplemented!("unexpected request: {:?}", req),
                }
            }
        }

        let rng = &mut TestRng::new();
        let block = TestBlockBuilder::new().build(rng);
        let bid = BidKind::Validator(ValidatorBid::empty(PublicKey::random(rng), rng.gen()).into());
        let legacy_bid = Bid::empty(PublicKey::random(rng), rng.gen());

        let resp = GetAuctionInfo::do_handle_request(
            Arc::new(ClientMock {
                block: Block::V2(block.clone()),
                bids: vec![bid.clone()],
                legacy_bids: vec![legacy_bid.clone()],
                contract_hash: rng.gen(),
                snapshot: Default::default(),
            }),
            None,
        )
        .await
        .expect("should handle request");

        assert_eq!(
            resp,
            GetAuctionInfoResult {
                api_version: CURRENT_API_VERSION,
                auction_state: AuctionState::new(
                    *block.state_root_hash(),
                    block.height(),
                    Default::default(),
                    vec![bid, BidKind::Unified(legacy_bid.into())]
                ),
            }
        );
    }

    #[tokio::test]
    async fn should_read_pre_1_5_auction_info() {
        struct ClientMock {
            block: Block,
            bids: Vec<BidKind>,
            legacy_bids: Vec<Bid>,
            contract_hash: AddressableEntityHash,
            snapshot: SeigniorageRecipientsSnapshot,
        }

        #[async_trait]
        impl NodeClient for ClientMock {
            async fn send_request(
                &self,
                req: BinaryRequest,
            ) -> Result<BinaryResponseAndRequest, ClientError> {
                match req {
                    BinaryRequest::Get(GetRequest::Information { info_type_tag, .. })
                        if InformationRequestTag::try_from(info_type_tag)
                            == Ok(InformationRequestTag::BlockHeader) =>
                    {
                        Ok(BinaryResponseAndRequest::new(
                            BinaryResponse::from_value(
                                self.block.clone_header(),
                                SUPPORTED_PROTOCOL_VERSION,
                            ),
                            &[],
                        ))
                    }
                    BinaryRequest::Get(GetRequest::State(req))
                        if matches!(
                            &*req,
                            GlobalStateRequest::AllItems {
                                key_tag: KeyTag::Bid,
                                ..
                            }
                        ) =>
                    {
                        let bids = self
                            .legacy_bids
                            .iter()
                            .cloned()
                            .map(|bid| StoredValue::Bid(bid.into()))
                            .collect::<Vec<_>>();
                        Ok(BinaryResponseAndRequest::new(
                            BinaryResponse::from_value(bids, SUPPORTED_PROTOCOL_VERSION),
                            &[],
                        ))
                    }
                    BinaryRequest::Get(GetRequest::State(req))
                        if matches!(
                            &*req,
                            GlobalStateRequest::AllItems {
                                key_tag: KeyTag::BidAddr,
                                ..
                            }
                        ) =>
                    {
                        let bids = self
                            .bids
                            .iter()
                            .cloned()
                            .map(StoredValue::BidKind)
                            .collect::<Vec<_>>();
                        Ok(BinaryResponseAndRequest::new(
                            BinaryResponse::from_value(bids, SUPPORTED_PROTOCOL_VERSION),
                            &[],
                        ))
                    }
                    BinaryRequest::Get(GetRequest::State(req))
                        if matches!(
                            &*req,
                            GlobalStateRequest::Item {
                                base_key: Key::SystemEntityRegistry,
                                // system entity registry is not present in pre-1.5 state
                                state_identifier: None,
                                ..
                            }
                        ) =>
                    {
                        let system_contracts =
                            iter::once((AUCTION.to_string(), self.contract_hash))
                                .collect::<BTreeMap<_, _>>();
                        let result = GlobalStateQueryResult::new(
                            StoredValue::CLValue(CLValue::from_t(system_contracts).unwrap()),
                            vec![],
                        );
                        Ok(BinaryResponseAndRequest::new(
                            BinaryResponse::from_value(result, SUPPORTED_PROTOCOL_VERSION),
                            &[],
                        ))
                    }
                    BinaryRequest::Get(GetRequest::State(req))
                        if matches!(
                            &*req,
                            GlobalStateRequest::Item {
                                // we should return nothing for entity hash in pre-1.5 state
                                base_key: Key::AddressableEntity(_),
                                ..
                            }
                        ) =>
                    {
                        Ok(BinaryResponseAndRequest::new(
                            BinaryResponse::new_empty(SUPPORTED_PROTOCOL_VERSION),
                            &[],
                        ))
                    }
                    BinaryRequest::Get(GetRequest::State(req))
                        if matches!(
                            &*req,
                            GlobalStateRequest::Item {
                                // we should query by contract hash in pre-1.5 state
                                base_key: Key::Hash(_),
                                ..
                            }
                        ) =>
                    {
                        let result = GlobalStateQueryResult::new(
                            StoredValue::CLValue(CLValue::from_t(self.snapshot.clone()).unwrap()),
                            vec![],
                        );
                        Ok(BinaryResponseAndRequest::new(
                            BinaryResponse::from_value(result, SUPPORTED_PROTOCOL_VERSION),
                            &[],
                        ))
                    }
                    req => unimplemented!("unexpected request: {:?}", req),
                }
            }
        }

        let rng = &mut TestRng::new();
        let block = TestBlockBuilder::new().build(rng);
        let bid = BidKind::Validator(ValidatorBid::empty(PublicKey::random(rng), rng.gen()).into());
        let legacy_bid = Bid::empty(PublicKey::random(rng), rng.gen());

        let resp = GetAuctionInfo::do_handle_request(
            Arc::new(ClientMock {
                block: Block::V2(block.clone()),
                bids: vec![bid.clone()],
                legacy_bids: vec![legacy_bid.clone()],
                contract_hash: rng.gen(),
                snapshot: Default::default(),
            }),
            None,
        )
        .await
        .expect("should handle request");

        assert_eq!(
            resp,
            GetAuctionInfoResult {
                api_version: CURRENT_API_VERSION,
                auction_state: AuctionState::new(
                    *block.state_root_hash(),
                    block.height(),
                    Default::default(),
                    vec![bid, BidKind::Unified(legacy_bid.into())]
                ),
            }
        );
    }

    #[tokio::test]
    async fn should_fail_auction_info_when_block_not_found() {
        struct ClientMock;

        #[async_trait]
        impl NodeClient for ClientMock {
            async fn send_request(
                &self,
                req: BinaryRequest,
            ) -> Result<BinaryResponseAndRequest, ClientError> {
                match req {
                    BinaryRequest::Get(GetRequest::Information { info_type_tag, .. })
                        if InformationRequestTag::try_from(info_type_tag)
                            == Ok(InformationRequestTag::BlockHeader) =>
                    {
                        Ok(BinaryResponseAndRequest::new(
                            BinaryResponse::new_empty(SUPPORTED_PROTOCOL_VERSION),
                            &[],
                        ))
                    }
                    BinaryRequest::Get(GetRequest::Information { info_type_tag, .. })
                        if InformationRequestTag::try_from(info_type_tag)
                            == Ok(InformationRequestTag::AvailableBlockRange) =>
                    {
                        Ok(BinaryResponseAndRequest::new(
                            BinaryResponse::from_value(
                                AvailableBlockRange::RANGE_0_0,
                                SUPPORTED_PROTOCOL_VERSION,
                            ),
                            &[],
                        ))
                    }
                    req => unimplemented!("unexpected request: {:?}", req),
                }
            }
        }

        let err = GetAuctionInfo::do_handle_request(Arc::new(ClientMock), None)
            .await
            .expect_err("should reject request");

        assert_eq!(err.code(), ErrorCode::NoSuchBlock as i64);
    }

    #[tokio::test]
    async fn should_read_entity() {
        use casper_types::addressable_entity::{ActionThresholds, AssociatedKeys};

        struct ClientMock {
            entity: AddressableEntity,
            named_keys: NamedKeys,
            entry_points: Vec<EntryPointValue>,
            entity_hash: AddressableEntityHash,
        }

        #[async_trait]
        impl NodeClient for ClientMock {
            async fn send_request(
                &self,
                req: BinaryRequest,
            ) -> Result<BinaryResponseAndRequest, ClientError> {
                match req {
                    BinaryRequest::Get(GetRequest::State(req))
                        if matches!(
                            &*req,
                            GlobalStateRequest::Item {
                                base_key: Key::Account(_),
                                ..
                            }
                        ) =>
                    {
                        Ok(BinaryResponseAndRequest::new(
                            BinaryResponse::from_value(
                                GlobalStateQueryResult::new(
                                    StoredValue::CLValue(
                                        CLValue::from_t(Key::contract_entity_key(self.entity_hash))
                                            .unwrap(),
                                    ),
                                    vec![],
                                ),
                                SUPPORTED_PROTOCOL_VERSION,
                            ),
                            &[],
                        ))
                    }
                    BinaryRequest::Get(GetRequest::State(req))
                        if matches!(
                            &*req,
                            GlobalStateRequest::Item {
                                base_key: Key::AddressableEntity(_),
                                ..
                            }
                        ) =>
                    {
                        Ok(BinaryResponseAndRequest::new(
                            BinaryResponse::from_value(
                                GlobalStateQueryResult::new(
                                    StoredValue::AddressableEntity(self.entity.clone()),
                                    vec![],
                                ),
                                SUPPORTED_PROTOCOL_VERSION,
                            ),
                            &[],
                        ))
                    }
                    BinaryRequest::Get(GetRequest::State(req))
                        if matches!(
                            &*req,
                            GlobalStateRequest::ItemsByPrefix {
                                key_prefix: KeyPrefix::NamedKeysByEntity(_),
                                ..
                            }
                        ) =>
                    {
                        Ok(BinaryResponseAndRequest::new(
                            BinaryResponse::from_value(
                                self.named_keys
                                    .iter()
                                    .map(|(name, key)| {
                                        StoredValue::NamedKey(
                                            NamedKeyValue::from_concrete_values(*key, name.clone())
                                                .expect("should create named key"),
                                        )
                                    })
                                    .collect::<Vec<_>>(),
                                SUPPORTED_PROTOCOL_VERSION,
                            ),
                            &[],
                        ))
                    }
                    BinaryRequest::Get(GetRequest::State(req))
                        if matches!(
                            &*req,
                            GlobalStateRequest::ItemsByPrefix {
                                key_prefix: KeyPrefix::EntryPointsV1ByEntity(_),
                                ..
                            }
                        ) =>
                    {
                        Ok(BinaryResponseAndRequest::new(
                            BinaryResponse::from_value(
                                self.entry_points
                                    .iter()
                                    .cloned()
                                    .map(StoredValue::EntryPoint)
                                    .collect::<Vec<_>>(),
                                SUPPORTED_PROTOCOL_VERSION,
                            ),
                            &[],
                        ))
                    }
                    BinaryRequest::Get(GetRequest::State(req))
                        if matches!(
                            &*req,
                            GlobalStateRequest::ItemsByPrefix {
                                key_prefix: KeyPrefix::EntryPointsV2ByEntity(_),
                                ..
                            }
                        ) =>
                    {
                        Ok(BinaryResponseAndRequest::new(
                            BinaryResponse::from_value(
                                Vec::<StoredValue>::new(),
                                SUPPORTED_PROTOCOL_VERSION,
                            ),
                            &[],
                        ))
                    }
                    req => unimplemented!("unexpected request: {:?}", req),
                }
            }
        }

        let rng = &mut TestRng::new();
        let entity = AddressableEntity::new(
            PackageHash::new(rng.gen()),
            ByteCodeHash::new(rng.gen()),
            ProtocolVersion::V1_0_0,
            rng.gen(),
            AssociatedKeys::default(),
            ActionThresholds::default(),
            MessageTopics::default(),
            EntityKind::SmartContract(TransactionRuntime::VmCasperV2),
        );
        let entity_hash: AddressableEntityHash = rng.gen();

        let named_key_count = rng.gen_range(0..10);
        let named_keys: NamedKeys =
            iter::repeat_with(|| (rng.random_string(1..36), Key::Hash(rng.gen())))
                .take(named_key_count)
                .collect::<BTreeMap<_, _>>()
                .into();
        let entry_point_count = rng.gen_range(0..10);
        let entry_points = iter::repeat_with(|| {
            EntryPointValue::new_v1_entry_point_value(EntryPoint::default_with_name(
                rng.random_string(1..10),
            ))
        })
        .take(entry_point_count)
        .collect::<Vec<_>>();

        let entity_identifier = EntityIdentifier::random(rng);

        let resp = GetAddressableEntity::do_handle_request(
            Arc::new(ClientMock {
                entity: entity.clone(),
                named_keys: named_keys.clone(),
                entry_points: entry_points.clone(),
                entity_hash,
            }),
            GetAddressableEntityParams {
                block_identifier: None,
                entity_identifier,
            },
        )
        .await
        .expect("should handle request");

        assert_eq!(
            resp,
            GetAddressableEntityResult {
                api_version: CURRENT_API_VERSION,
                entity: EntityOrAccount::AddressableEntity {
                    entity,
                    named_keys,
                    entry_points
                },
                merkle_proof: String::from("00000000"),
            }
        );
    }

    #[tokio::test]
    async fn should_read_entity_legacy_account() {
        use casper_types::account::{ActionThresholds, AssociatedKeys};

        let rng = &mut TestRng::new();
        let block = Block::V2(TestBlockBuilder::new().build(rng));
        let account = Account::new(
            rng.gen(),
            NamedKeys::default(),
            rng.gen(),
            AssociatedKeys::default(),
            ActionThresholds::default(),
        );
        let entity_identifier = EntityIdentifier::AccountHash(rng.gen());

        let resp = GetAddressableEntity::do_handle_request(
            Arc::new(ValidLegacyAccountMock {
                block: block.clone(),
                account: account.clone(),
            }),
            GetAddressableEntityParams {
                block_identifier: None,
                entity_identifier,
            },
        )
        .await
        .expect("should handle request");

        assert_eq!(
            resp,
            GetAddressableEntityResult {
                api_version: CURRENT_API_VERSION,
                entity: EntityOrAccount::LegacyAccount(account),
                merkle_proof: String::from("00000000"),
            }
        );
    }

    #[tokio::test]
    async fn should_reject_read_entity_when_non_existent() {
        struct ClientMock {
            block: Block,
        }

        #[async_trait]
        impl NodeClient for ClientMock {
            async fn send_request(
                &self,
                req: BinaryRequest,
            ) -> Result<BinaryResponseAndRequest, ClientError> {
                match req {
                    BinaryRequest::Get(GetRequest::Information { info_type_tag, .. })
                        if InformationRequestTag::try_from(info_type_tag)
                            == Ok(InformationRequestTag::BlockHeader) =>
                    {
                        Ok(BinaryResponseAndRequest::new(
                            BinaryResponse::from_value(
                                self.block.clone_header(),
                                SUPPORTED_PROTOCOL_VERSION,
                            ),
                            &[],
                        ))
                    }
                    BinaryRequest::Get(GetRequest::State(req))
                        if matches!(
                            &*req,
                            GlobalStateRequest::Item {
                                base_key: Key::AddressableEntity(_),
                                ..
                            }
                        ) =>
                    {
                        Ok(BinaryResponseAndRequest::new(
                            BinaryResponse::new_empty(SUPPORTED_PROTOCOL_VERSION),
                            &[],
                        ))
                    }
                    req => unimplemented!("unexpected request: {:?}", req),
                }
            }
        }

        let rng = &mut TestRng::new();
        let block = Block::V2(TestBlockBuilder::new().build(rng));
        let entity_identifier = EntityIdentifier::EntityAddr(rng.gen());

        let err = GetAddressableEntity::do_handle_request(
            Arc::new(ClientMock {
                block: block.clone(),
            }),
            GetAddressableEntityParams {
                block_identifier: None,
                entity_identifier,
            },
        )
        .await
        .expect_err("should reject request");

        assert_eq!(err.code(), ErrorCode::NoSuchAddressableEntity as i64);
    }

    #[tokio::test]
    async fn should_read_account_info() {
        use casper_types::account::{ActionThresholds, AssociatedKeys};

        let rng = &mut TestRng::new();
        let block = Block::V2(TestBlockBuilder::new().build(rng));
        let account = Account::new(
            rng.gen(),
            NamedKeys::default(),
            rng.gen(),
            AssociatedKeys::default(),
            ActionThresholds::default(),
        );
        let account_identifier = AccountIdentifier::random(rng);

        let resp = GetAccountInfo::do_handle_request(
            Arc::new(ValidLegacyAccountMock {
                block: block.clone(),
                account: account.clone(),
            }),
            GetAccountInfoParams {
                block_identifier: None,
                account_identifier,
            },
        )
        .await
        .expect("should handle request");

        assert_eq!(
            resp,
            GetAccountInfoResult {
                api_version: CURRENT_API_VERSION,
                account,
                merkle_proof: String::from("00000000"),
            }
        );
    }

    #[tokio::test]
    async fn should_reject_read_account_info_when_migrated() {
        struct ClientMock {
            block: Block,
            entity_hash: AddressableEntityHash,
        }

        #[async_trait]
        impl NodeClient for ClientMock {
            async fn send_request(
                &self,
                req: BinaryRequest,
            ) -> Result<BinaryResponseAndRequest, ClientError> {
                match req {
                    BinaryRequest::Get(GetRequest::Information { info_type_tag, .. })
                        if InformationRequestTag::try_from(info_type_tag)
                            == Ok(InformationRequestTag::BlockHeader) =>
                    {
                        Ok(BinaryResponseAndRequest::new(
                            BinaryResponse::from_value(
                                self.block.clone_header(),
                                SUPPORTED_PROTOCOL_VERSION,
                            ),
                            &[],
                        ))
                    }
                    BinaryRequest::Get(GetRequest::State(req))
                        if matches!(
                            &*req,
                            GlobalStateRequest::Item {
                                base_key: Key::Account(_),
                                ..
                            }
                        ) =>
                    {
                        Ok(BinaryResponseAndRequest::new(
                            BinaryResponse::from_value(
                                GlobalStateQueryResult::new(
                                    StoredValue::CLValue(
                                        CLValue::from_t(Key::contract_entity_key(self.entity_hash))
                                            .unwrap(),
                                    ),
                                    vec![],
                                ),
                                SUPPORTED_PROTOCOL_VERSION,
                            ),
                            &[],
                        ))
                    }
                    req => unimplemented!("unexpected request: {:?}", req),
                }
            }
        }

        let rng = &mut TestRng::new();
        let block = Block::V2(TestBlockBuilder::new().build(rng));
        let entity_hash: AddressableEntityHash = rng.gen();
        let account_identifier = AccountIdentifier::random(rng);

        let err = GetAccountInfo::do_handle_request(
            Arc::new(ClientMock {
                block: block.clone(),
                entity_hash,
            }),
            GetAccountInfoParams {
                block_identifier: None,
                account_identifier,
            },
        )
        .await
        .expect_err("should reject request");

        assert_eq!(err.code(), ErrorCode::AccountMigratedToEntity as i64);
    }

    #[tokio::test]
    async fn should_reject_read_account_info_when_non_existent() {
        struct ClientMock {
            block: Block,
        }

        #[async_trait]
        impl NodeClient for ClientMock {
            async fn send_request(
                &self,
                req: BinaryRequest,
            ) -> Result<BinaryResponseAndRequest, ClientError> {
                match req {
                    BinaryRequest::Get(GetRequest::Information { info_type_tag, .. })
                        if InformationRequestTag::try_from(info_type_tag)
                            == Ok(InformationRequestTag::BlockHeader) =>
                    {
                        Ok(BinaryResponseAndRequest::new(
                            BinaryResponse::from_value(
                                self.block.clone_header(),
                                SUPPORTED_PROTOCOL_VERSION,
                            ),
                            &[],
                        ))
                    }
                    BinaryRequest::Get(GetRequest::State(req))
                        if matches!(
                            &*req,
                            GlobalStateRequest::Item {
                                base_key: Key::Account(_),
                                ..
                            }
                        ) =>
                    {
                        Ok(BinaryResponseAndRequest::new(
                            BinaryResponse::new_empty(SUPPORTED_PROTOCOL_VERSION),
                            &[],
                        ))
                    }
                    req => unimplemented!("unexpected request: {:?}", req),
                }
            }
        }

        let rng = &mut TestRng::new();
        let block = Block::V2(TestBlockBuilder::new().build(rng));
        let account_identifier = AccountIdentifier::random(rng);

        let err = GetAccountInfo::do_handle_request(
            Arc::new(ClientMock {
                block: block.clone(),
            }),
            GetAccountInfoParams {
                block_identifier: None,
                account_identifier,
            },
        )
        .await
        .expect_err("should reject request");

        assert_eq!(err.code(), ErrorCode::NoSuchAccount as i64);
    }

    #[tokio::test]
    async fn should_read_dictionary_item() {
        let rng = &mut TestRng::new();
        let stored_value = StoredValue::CLValue(CLValue::from_t(rng.gen::<i32>()).unwrap());

        let uref = URef::new(rng.gen(), AccessRights::empty());
        let item_key = rng.random_string(5..10);
        let query_result = GlobalStateQueryResult::new(stored_value.clone(), vec![]);
        let dict_key = Key::dictionary(uref, item_key.as_bytes());

        let resp = GetDictionaryItem::do_handle_request(
            Arc::new(ValidDictionaryQueryResultMock {
                dict_key,
                query_result,
            }),
            GetDictionaryItemParams {
                state_root_hash: rng.gen(),
                dictionary_identifier: DictionaryIdentifier::URef {
                    seed_uref: uref.to_formatted_string(),
                    dictionary_item_key: item_key.clone(),
                },
            },
        )
        .await
        .expect("should handle request");

        assert_eq!(
            resp,
            GetDictionaryItemResult {
                api_version: CURRENT_API_VERSION,
                dictionary_key: dict_key.to_formatted_string(),
                stored_value,
                merkle_proof: String::from("00000000"),
            }
        );
    }

    #[tokio::test]
    async fn should_read_query_global_state_result() {
        let rng = &mut TestRng::new();
        let block = Block::V2(TestBlockBuilder::new().build(rng));
        let stored_value = StoredValue::CLValue(CLValue::from_t(rng.gen::<i32>()).unwrap());
        let expected = GlobalStateQueryResult::new(stored_value.clone(), vec![]);

        let resp = QueryGlobalState::do_handle_request(
            Arc::new(ValidGlobalStateResultWithBlockMock {
                block: block.clone(),
                result: expected.clone(),
            }),
            QueryGlobalStateParams {
                state_identifier: Some(GlobalStateIdentifier::BlockHash(*block.hash())),
                key: rng.gen(),
                path: vec![],
            },
        )
        .await
        .expect("should handle request");

        assert_eq!(
            resp,
            QueryGlobalStateResult {
                api_version: CURRENT_API_VERSION,
                block_header: Some(block.take_header()),
                stored_value,
                merkle_proof: String::from("00000000"),
            }
        );
    }

    #[tokio::test]
    async fn should_read_query_balance_result() {
        let rng = &mut TestRng::new();
        let available_balance = rng.gen();
        let total_balance = rng.gen();
        let balance = BalanceResponse {
            total_balance,
            available_balance,
            total_balance_proof: Box::new(TrieMerkleProof::new(
                Key::Account(rng.gen()),
                StoredValue::CLValue(CLValue::from_t(rng.gen::<i32>()).unwrap()),
                VecDeque::from_iter([TrieMerkleProofStep::random(rng)]),
            )),
            balance_holds: BTreeMap::new(),
        };

        let resp = QueryBalance::do_handle_request(
            Arc::new(ValidBalanceMock(balance.clone())),
            QueryBalanceParams {
                state_identifier: Some(GlobalStateIdentifier::random(rng)),
                purse_identifier: PurseIdentifier::PurseUref(URef::new(
                    rng.gen(),
                    AccessRights::empty(),
                )),
            },
        )
        .await
        .expect("should handle request");

        assert_eq!(
            resp,
            QueryBalanceResult {
                api_version: CURRENT_API_VERSION,
                balance: available_balance,
            }
        );
    }

    #[tokio::test]
    async fn should_read_query_balance_details_result() {
        let rng = &mut TestRng::new();
        let available_balance = rng.gen();
        let total_balance = rng.gen();
        let balance = BalanceResponse {
            total_balance,
            available_balance,
            total_balance_proof: Box::new(TrieMerkleProof::new(
                Key::Account(rng.gen()),
                StoredValue::CLValue(CLValue::from_t(rng.gen::<i32>()).unwrap()),
                VecDeque::from_iter([TrieMerkleProofStep::random(rng)]),
            )),
            balance_holds: BTreeMap::new(),
        };

        let resp = QueryBalanceDetails::do_handle_request(
            Arc::new(ValidBalanceMock(balance.clone())),
            QueryBalanceDetailsParams {
                state_identifier: Some(GlobalStateIdentifier::random(rng)),
                purse_identifier: PurseIdentifier::PurseUref(URef::new(
                    rng.gen(),
                    AccessRights::empty(),
                )),
            },
        )
        .await
        .expect("should handle request");

        assert_eq!(
            resp,
            QueryBalanceDetailsResult {
                api_version: CURRENT_API_VERSION,
                total_balance,
                available_balance,
                total_balance_proof: common::encode_proof(&vec![*balance.total_balance_proof])
                    .expect("should encode proof"),
                holds: vec![],
            }
        );
    }

    struct ValidDictionaryQueryResultMock {
        dict_key: Key,
        query_result: GlobalStateQueryResult,
    }

    #[async_trait]
    impl NodeClient for ValidDictionaryQueryResultMock {
        async fn send_request(
            &self,
            req: BinaryRequest,
        ) -> Result<BinaryResponseAndRequest, ClientError> {
            match req {
                BinaryRequest::Get(GetRequest::State(req))
                    if matches!(&*req, GlobalStateRequest::DictionaryItem { .. }) =>
                {
                    Ok(BinaryResponseAndRequest::new(
                        BinaryResponse::from_value(
                            DictionaryQueryResult::new(self.dict_key, self.query_result.clone()),
                            SUPPORTED_PROTOCOL_VERSION,
                        ),
                        &[],
                    ))
                }
                req => unimplemented!("unexpected request: {:?}", req),
            }
        }
    }

    struct ValidGlobalStateResultMock(GlobalStateQueryResult);

    #[async_trait]
    impl NodeClient for ValidGlobalStateResultMock {
        async fn send_request(
            &self,
            req: BinaryRequest,
        ) -> Result<BinaryResponseAndRequest, ClientError> {
            match req {
                BinaryRequest::Get(GetRequest::State(req))
                    if matches!(&*req, GlobalStateRequest::Item { .. }) =>
                {
                    Ok(BinaryResponseAndRequest::new(
                        BinaryResponse::from_value(self.0.clone(), SUPPORTED_PROTOCOL_VERSION),
                        &[],
                    ))
                }
                req => unimplemented!("unexpected request: {:?}", req),
            }
        }
    }

    struct ValidGlobalStateResultWithBlockMock {
        block: Block,
        result: GlobalStateQueryResult,
    }

    #[async_trait]
    impl NodeClient for ValidGlobalStateResultWithBlockMock {
        async fn send_request(
            &self,
            req: BinaryRequest,
        ) -> Result<BinaryResponseAndRequest, ClientError> {
            match req {
                BinaryRequest::Get(GetRequest::Information { info_type_tag, .. })
                    if InformationRequestTag::try_from(info_type_tag)
                        == Ok(InformationRequestTag::BlockHeader) =>
                {
                    Ok(BinaryResponseAndRequest::new(
                        BinaryResponse::from_value(
                            self.block.clone_header(),
                            SUPPORTED_PROTOCOL_VERSION,
                        ),
                        &[],
                    ))
                }
                BinaryRequest::Get(GetRequest::State(req))
                    if matches!(&*req, GlobalStateRequest::Item { .. }) =>
                {
                    Ok(BinaryResponseAndRequest::new(
                        BinaryResponse::from_value(self.result.clone(), SUPPORTED_PROTOCOL_VERSION),
                        &[],
                    ))
                }
                req => unimplemented!("unexpected request: {:?}", req),
            }
        }
    }

    struct ValidLegacyAccountMock {
        block: Block,
        account: Account,
    }

    #[async_trait]
    impl NodeClient for ValidLegacyAccountMock {
        async fn send_request(
            &self,
            req: BinaryRequest,
        ) -> Result<BinaryResponseAndRequest, ClientError> {
            match req {
                BinaryRequest::Get(GetRequest::Information { info_type_tag, .. })
                    if InformationRequestTag::try_from(info_type_tag)
                        == Ok(InformationRequestTag::BlockHeader) =>
                {
                    Ok(BinaryResponseAndRequest::new(
                        BinaryResponse::from_value(
                            self.block.clone_header(),
                            SUPPORTED_PROTOCOL_VERSION,
                        ),
                        &[],
                    ))
                }
                BinaryRequest::Get(GetRequest::State(req))
                    if matches!(
                        &*req,
                        GlobalStateRequest::Item {
                            base_key: Key::Account(_),
                            ..
                        }
                    ) =>
                {
                    Ok(BinaryResponseAndRequest::new(
                        BinaryResponse::from_value(
                            GlobalStateQueryResult::new(
                                StoredValue::Account(self.account.clone()),
                                vec![],
                            ),
                            SUPPORTED_PROTOCOL_VERSION,
                        ),
                        &[],
                    ))
                }
                req => unimplemented!("unexpected request: {:?}", req),
            }
        }
    }

    struct ValidBalanceMock(BalanceResponse);

    #[async_trait]
    impl NodeClient for ValidBalanceMock {
        async fn send_request(
            &self,
            req: BinaryRequest,
        ) -> Result<BinaryResponseAndRequest, ClientError> {
            match req {
                BinaryRequest::Get(GetRequest::State(req))
                    if matches!(&*req, GlobalStateRequest::Balance { .. }) =>
                {
                    Ok(BinaryResponseAndRequest::new(
                        BinaryResponse::from_value(self.0.clone(), SUPPORTED_PROTOCOL_VERSION),
                        &[],
                    ))
                }
                req => unimplemented!("unexpected request: {:?}", req),
            }
        }
    }
}
