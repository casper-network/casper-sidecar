//! RPCs of state_get_auction_info_v2.

use std::{collections::BTreeMap, str, sync::Arc};

use crate::rpcs::state::ERA_VALIDATORS;
use async_trait::async_trait;
use casper_types::system::auction::ValidatorBid;
use once_cell::sync::Lazy;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::common;
use super::state::{
    era_validators_from_snapshot, fetch_bid_kinds, GetAuctionInfoParams, JsonEraValidators,
    JsonValidatorWeight,
};
use super::{
    docs::{DocExample, DOCS_EXAMPLE_API_VERSION},
    ApiVersion, Error, NodeClient, RpcError, RpcWithOptionalParams, CURRENT_API_VERSION,
};
use casper_types::{
    addressable_entity::EntityKindTag,
    system::{
        auction::{BidKind, DelegatorBid, EraValidators, SEIGNIORAGE_RECIPIENTS_SNAPSHOT_KEY},
        AUCTION,
    },
    AddressableEntityHash, Digest, GlobalStateIdentifier, Key, PublicKey, U512,
};

static GET_AUCTION_INFO_RESULT: Lazy<GetAuctionInfoResult> = Lazy::new(|| GetAuctionInfoResult {
    api_version: DOCS_EXAMPLE_API_VERSION,
    auction_state: AuctionState::doc_example().clone(),
});
static AUCTION_INFO: Lazy<AuctionState> = Lazy::new(|| {
    use casper_types::{system::auction::DelegationRate, AccessRights, SecretKey, URef};
    use num_traits::Zero;

    let state_root_hash = Digest::from([11; Digest::LENGTH]);
    let validator_secret_key =
        SecretKey::ed25519_from_bytes([42; SecretKey::ED25519_LENGTH]).unwrap();
    let validator_public_key = PublicKey::from(&validator_secret_key);

    let mut bids = vec![];
    let validator_bid = ValidatorBid::unlocked(
        validator_public_key.clone(),
        URef::new([250; 32], AccessRights::READ_ADD_WRITE),
        U512::from(20),
        DelegationRate::zero(),
        0,
        u64::MAX,
        0,
    );
    bids.push(BidKind::Validator(Box::new(validator_bid)));

    let delegator_secret_key =
        SecretKey::ed25519_from_bytes([43; SecretKey::ED25519_LENGTH]).unwrap();
    let delegator_public_key = PublicKey::from(&delegator_secret_key);
    let delegator_bid = DelegatorBid::unlocked(
        delegator_public_key.into(),
        U512::from(10),
        URef::new([251; 32], AccessRights::READ_ADD_WRITE),
        validator_public_key,
    );
    bids.push(BidKind::Delegator(Box::new(delegator_bid)));

    let height: u64 = 10;
    let era_validators = ERA_VALIDATORS.clone();
    AuctionState::new(state_root_hash, height, era_validators, bids)
});

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

/// "state_get_auction_info_v2" RPC.
pub struct GetAuctionInfo {}

#[async_trait]
impl RpcWithOptionalParams for GetAuctionInfo {
    const METHOD: &'static str = "state_get_auction_info_v2";
    type OptionalRequestParams = GetAuctionInfoParams;
    type ResponseResult = GetAuctionInfoResult;

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        maybe_params: Option<Self::OptionalRequestParams>,
    ) -> Result<Self::ResponseResult, RpcError> {
        let block_identifier = maybe_params.map(|params| params.block_identifier);
        let block_header = common::get_block_header(&*node_client, block_identifier).await?;

        let state_identifier = block_identifier.map(GlobalStateIdentifier::from);
        let state_identifier =
            state_identifier.unwrap_or(GlobalStateIdentifier::BlockHeight(block_header.height()));

        let is_1x = block_header.protocol_version().value().major == 1;
        let bids = fetch_bid_kinds(node_client.clone(), state_identifier, is_1x).await?;

        // always retrieve the latest system contract registry, old versions of the node
        // did not write it to the global state
        let (registry_value, _) = node_client
            .query_global_state(Some(state_identifier), Key::SystemEntityRegistry, vec![])
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
                Some(state_identifier),
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
                    Some(state_identifier),
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
            .ok_or(Error::InvalidAuctionState)?;

        let validators = era_validators_from_snapshot(snapshot, is_1x)?;
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

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BidKindWrapper {
    public_key: PublicKey,
    bid: BidKind,
}

impl BidKindWrapper {
    /// ctor
    pub fn new(public_key: PublicKey, bid: BidKind) -> Self {
        Self { public_key, bid }
    }
}

/// Data structure summarizing auction contract data.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(rename = "AuctionStateV2")]
pub(crate) struct AuctionState {
    /// Global state hash.
    state_root_hash: Digest,
    /// Block height.
    block_height: u64,
    /// Era validators.
    era_validators: Vec<JsonEraValidators>,
    /// All bids.
    bids: Vec<BidKindWrapper>,
}

impl AuctionState {
    /// Ctor
    pub fn new(
        state_root_hash: Digest,
        block_height: u64,
        era_validators: EraValidators,
        bids: Vec<BidKind>,
    ) -> Self {
        let mut json_era_validators: Vec<JsonEraValidators> = Vec::new();
        for (era_id, validator_weights) in &era_validators {
            let mut json_validator_weights: Vec<JsonValidatorWeight> = Vec::new();
            for (public_key, weight) in validator_weights {
                json_validator_weights.push(JsonValidatorWeight::new(public_key.clone(), *weight));
            }
            json_era_validators.push(JsonEraValidators::new(*era_id, json_validator_weights));
        }

        let bids = bids
            .into_iter()
            .map(|bid_kind| BidKindWrapper::new(bid_kind.validator_public_key(), bid_kind))
            .collect();

        AuctionState {
            state_root_hash,
            block_height,
            era_validators: json_era_validators,
            bids,
        }
    }

    // This method is not intended to be used by third party crates.
    #[doc(hidden)]
    pub fn example() -> &'static Self {
        &AUCTION_INFO
    }
}

impl DocExample for AuctionState {
    fn doc_example() -> &'static Self {
        AuctionState::example()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        rpcs::{
            state_get_auction_info_v2::{AuctionState, GetAuctionInfo, GetAuctionInfoResult},
            test_utils::BinaryPortMock,
            RpcWithOptionalParams, CURRENT_API_VERSION,
        },
        SUPPORTED_PROTOCOL_VERSION,
    };
    use casper_binary_port::InformationRequest;
    use casper_types::{
        system::{
            auction::{
                Bid, BidKind, DelegatorKind, SeigniorageRecipientV1, SeigniorageRecipientV2,
                SeigniorageRecipientsV1, SeigniorageRecipientsV2, ValidatorBid,
            },
            AUCTION,
        },
        testing::TestRng,
        AddressableEntityHash, CLValue, EraId, PublicKey, StoredValue, TestBlockV1Builder, U512,
    };
    use rand::Rng;
    use std::{collections::BTreeMap, sync::Arc};

    #[tokio::test]
    async fn should_read_pre_condor_auction_info_with_addressable_entity_off() {
        let rng = &mut TestRng::new();
        let mut binary_port_mock = BinaryPortMock::new();
        let auction_hash: AddressableEntityHash = AddressableEntityHash::new(rng.gen());
        let block_header = TestBlockV1Builder::new()
            .build_versioned(rng)
            .clone_header();
        let registry = BTreeMap::from([(AUCTION.to_string(), auction_hash)]);
        let public_key_1 = PublicKey::random(rng);
        let public_key_2 = PublicKey::random(rng);
        let recipient_v1 = SeigniorageRecipientV1::new(
            U512::from(125),
            50,
            BTreeMap::from([(public_key_2, U512::from(500))]),
        );
        let recipients_1: BTreeMap<PublicKey, SeigniorageRecipientV1> =
            BTreeMap::from([(public_key_1.clone(), recipient_v1)]);
        let v1_recipients: BTreeMap<EraId, SeigniorageRecipientsV1> =
            BTreeMap::from([(EraId::new(100), recipients_1)]);
        let stored_value = StoredValue::CLValue(CLValue::from_t(v1_recipients.clone()).unwrap());
        let bid_1 = Bid::empty(PublicKey::random(rng), rng.gen());
        let bids = vec![bid_1];
        let state_identifier = Some(casper_types::GlobalStateIdentifier::BlockHeight(
            block_header.height(),
        ));
        binary_port_mock
            .add_block_header_req_res(block_header.clone(), InformationRequest::BlockHeader(None))
            .await;
        binary_port_mock
            .add_bids_fetch_res(bids.clone(), state_identifier)
            .await;
        binary_port_mock
            .add_system_registry(state_identifier, registry)
            .await;
        binary_port_mock
            .add_seigniorage_snapshot_under_addressable_entity(state_identifier, auction_hash, None)
            .await;
        binary_port_mock
            .add_seigniorage_snapshot_under_key_hash(
                state_identifier,
                auction_hash,
                Some(stored_value),
            )
            .await;
        let resp = GetAuctionInfo::do_handle_request(Arc::new(binary_port_mock), None)
            .await
            .expect("should handle request");
        let bids = bids
            .into_iter()
            .map(|b| BidKind::Unified(Box::new(b)))
            .collect();

        let expected_validators = BTreeMap::from([(
            EraId::new(100),
            BTreeMap::from([(public_key_1, U512::from(625))]),
        )]);

        assert_eq!(
            resp,
            GetAuctionInfoResult {
                api_version: CURRENT_API_VERSION,
                auction_state: AuctionState::new(
                    *block_header.state_root_hash(),
                    block_header.height(),
                    expected_validators,
                    bids
                ),
            }
        );
    }

    #[tokio::test]
    async fn should_read_condor_auction_info_with_addressable_entity_off() {
        let rng = &mut TestRng::new();
        let mut binary_port_mock = BinaryPortMock::new();
        let auction_hash: AddressableEntityHash = AddressableEntityHash::new(rng.gen());
        let block_header = TestBlockV1Builder::new()
            .protocol_version(SUPPORTED_PROTOCOL_VERSION)
            .build_versioned(rng)
            .clone_header();
        let registry = BTreeMap::from([(AUCTION.to_string(), auction_hash)]);
        let public_key_1 = PublicKey::random(rng);
        let public_key_2 = PublicKey::random(rng);
        let public_key_3 = PublicKey::random(rng);
        let delegator_kind_1 = DelegatorKind::PublicKey(public_key_2);
        let delegator_kind_2 = DelegatorKind::PublicKey(public_key_3);
        let recipient_v2 = SeigniorageRecipientV2::new(
            U512::from(125),
            50,
            BTreeMap::from([(delegator_kind_1, U512::from(500))]),
            BTreeMap::from([(delegator_kind_2, 75)]),
        );
        let recipients_1: BTreeMap<PublicKey, SeigniorageRecipientV2> =
            BTreeMap::from([(public_key_1.clone(), recipient_v2)]);
        let v2_recipients: BTreeMap<EraId, SeigniorageRecipientsV2> =
            BTreeMap::from([(EraId::new(100), recipients_1)]);
        let stored_value = StoredValue::CLValue(CLValue::from_t(v2_recipients.clone()).unwrap());
        let state_identifier = Some(casper_types::GlobalStateIdentifier::BlockHeight(
            block_header.height(),
        ));
        let validator_bid = ValidatorBid::empty(PublicKey::random(rng), rng.gen());
        let bid_kind_1 = BidKind::Validator(Box::new(validator_bid));
        let bid_kinds = vec![bid_kind_1];
        binary_port_mock
            .add_block_header_req_res(block_header.clone(), InformationRequest::BlockHeader(None))
            .await;
        binary_port_mock
            .add_bid_kinds_fetch_res(bid_kinds.clone(), state_identifier)
            .await;
        binary_port_mock
            .add_system_registry(state_identifier, registry)
            .await;
        binary_port_mock
            .add_seigniorage_snapshot_under_addressable_entity(state_identifier, auction_hash, None)
            .await;
        binary_port_mock
            .add_seigniorage_snapshot_under_key_hash(
                state_identifier,
                auction_hash,
                Some(stored_value),
            )
            .await;
        let resp = GetAuctionInfo::do_handle_request(Arc::new(binary_port_mock), None)
            .await
            .expect("should handle request");
        let expected_validators = BTreeMap::from([(
            EraId::new(100),
            BTreeMap::from([(public_key_1, U512::from(625))]),
        )]);

        assert_eq!(
            resp,
            GetAuctionInfoResult {
                api_version: CURRENT_API_VERSION,
                auction_state: AuctionState::new(
                    *block_header.state_root_hash(),
                    block_header.height(),
                    expected_validators,
                    bid_kinds
                ),
            }
        );
    }

    #[tokio::test]
    async fn should_read_condor_auction_info_with_addressable_entity_on() {
        let rng = &mut TestRng::new();
        let mut binary_port_mock = BinaryPortMock::new();
        let auction_hash: AddressableEntityHash = AddressableEntityHash::new(rng.gen());
        let block_header = TestBlockV1Builder::new()
            .protocol_version(SUPPORTED_PROTOCOL_VERSION)
            .build_versioned(rng)
            .clone_header();
        let registry = BTreeMap::from([(AUCTION.to_string(), auction_hash)]);
        let public_key_1 = PublicKey::random(rng);
        let public_key_2 = PublicKey::random(rng);
        let public_key_3 = PublicKey::random(rng);
        let delegator_kind_1 = DelegatorKind::PublicKey(public_key_2);
        let delegator_kind_2 = DelegatorKind::PublicKey(public_key_3);
        let recipient_v2 = SeigniorageRecipientV2::new(
            U512::from(125),
            50,
            BTreeMap::from([(delegator_kind_1, U512::from(500))]),
            BTreeMap::from([(delegator_kind_2, 75)]),
        );
        let recipients_1: BTreeMap<PublicKey, SeigniorageRecipientV2> =
            BTreeMap::from([(public_key_1.clone(), recipient_v2)]);
        let v2_recipients: BTreeMap<EraId, SeigniorageRecipientsV2> =
            BTreeMap::from([(EraId::new(100), recipients_1)]);
        let stored_value = StoredValue::CLValue(CLValue::from_t(v2_recipients.clone()).unwrap());
        let state_identifier = Some(casper_types::GlobalStateIdentifier::BlockHeight(
            block_header.height(),
        ));
        let validator_bid = ValidatorBid::empty(PublicKey::random(rng), rng.gen());
        let bid_kind_1 = BidKind::Validator(Box::new(validator_bid));
        let bid_kinds = vec![bid_kind_1];
        binary_port_mock
            .add_block_header_req_res(block_header.clone(), InformationRequest::BlockHeader(None))
            .await;
        binary_port_mock
            .add_bid_kinds_fetch_res(bid_kinds.clone(), state_identifier)
            .await;
        binary_port_mock
            .add_system_registry(state_identifier, registry)
            .await;
        binary_port_mock
            .add_seigniorage_snapshot_under_addressable_entity(
                state_identifier,
                auction_hash,
                Some(stored_value),
            )
            .await;
        let resp = GetAuctionInfo::do_handle_request(Arc::new(binary_port_mock), None)
            .await
            .expect("should handle request");
        let expected_validators = BTreeMap::from([(
            EraId::new(100),
            BTreeMap::from([(public_key_1, U512::from(625))]),
        )]);

        assert_eq!(
            resp,
            GetAuctionInfoResult {
                api_version: CURRENT_API_VERSION,
                auction_state: AuctionState::new(
                    *block_header.state_root_hash(),
                    block_header.height(),
                    expected_validators,
                    bid_kinds
                ),
            }
        );
    }
}
