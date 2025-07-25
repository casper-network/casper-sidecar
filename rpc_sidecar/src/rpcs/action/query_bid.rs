use async_trait::async_trait;
use casper_json_rpc::Error as RpcError;
use casper_types::{
    BlockV2, EraId, GlobalStateIdentifier, PublicKey, SecretKey,
    system::auction::{BidKind, Bridge, DelegatorKind},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, LazyLock};

use crate::{
    NodeClient,
    node_client::BidQueryResponse,
    rpcs::{
        ApiVersion, CURRENT_API_VERSION, Error, RpcWithParams,
        common::BidQueryResult,
        docs::{DOCS_EXAMPLE_API_VERSION, DocExample},
    },
};

/// Polymorphic query that we can send to ask the bids state
#[derive(Serialize, Deserialize, Debug, JsonSchema, Clone)]
pub enum QueryBid {
    /// This variant will query the global state for validator data.
    ValidatorBid {
        /// Public key of the queried validator
        public_key: Box<PublicKey>,
    },
    DelegatorBid {
        /// Public key of the queried validator
        validator_public_key: Box<PublicKey>,
        /// Identifier of the queried delegator
        delegator: Box<DelegatorKind>,
    },
}

/// Parameters for "state_query_bids" RPC request.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct QueryBidsParams {
    /// State root hash.
    state_identifier: Option<GlobalStateIdentifier>,
    /// Actual part of the bid data that we want to ask for
    query: QueryBid,
}

static GET_QUERY_BIDS_PARAMS: LazyLock<QueryBidsParams> = LazyLock::new(|| {
    let secret_key = SecretKey::ed25519_from_bytes([0; 32]).unwrap();
    let public_key = Box::new(PublicKey::from(&secret_key));
    QueryBidsParams {
        state_identifier: Some(GlobalStateIdentifier::BlockHash(*BlockV2::example().hash())),
        query: QueryBid::ValidatorBid { public_key },
    }
});

static GET_QUERY_BIDS_RESULT: LazyLock<QueryBidsResult> = LazyLock::new(|| {
    let secret_key_1 = SecretKey::ed25519_from_bytes([0; 32]).unwrap();
    let public_key_1 = PublicKey::from(&secret_key_1);
    let secret_key_2 = SecretKey::ed25519_from_bytes([1; 32]).unwrap();
    let public_key_2 = PublicKey::from(&secret_key_2);

    let bridge = Bridge::new(public_key_1, public_key_2, EraId::new(1253));
    let bid_records = BidQueryResult::new(vec![BidKind::Bridge(Box::new(bridge))]);
    QueryBidsResult {
        api_version: DOCS_EXAMPLE_API_VERSION,
        bid_records,
    }
});

impl DocExample for QueryBidsParams {
    fn doc_example() -> &'static Self {
        &GET_QUERY_BIDS_PARAMS
    }
}

/// Result for "state_query_bids" RPC response.
#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct QueryBidsResult {
    /// The RPC API version.
    #[schemars(with = "String")]
    pub(crate) api_version: ApiVersion,
    /// Bid information.
    pub(crate) bid_records: BidQueryResult,
}

impl DocExample for QueryBidsResult {
    fn doc_example() -> &'static Self {
        &GET_QUERY_BIDS_RESULT
    }
}

/// `state_query_bids` RPC.
pub(crate) struct QueryBids {}

#[async_trait]
impl RpcWithParams for QueryBids {
    const METHOD: &'static str = "state_query_bids";
    type RequestParams = QueryBidsParams;
    type ResponseResult = QueryBidsResult;

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: Self::RequestParams,
    ) -> Result<Self::ResponseResult, RpcError> {
        let resp = match node_client
            .query_bids(params.state_identifier, params.query)
            .await
            .map_err(|err| Error::NodeRequest("query bid", err))?
            .ok_or(Error::BidQueryNoResponse)?
        {
            BidQueryResponse::Validator(validator_bid_information) => {
                let bids = validator_bid_information.bids();
                BidQueryResult::new(bids.clone())
            }
            BidQueryResponse::Delegator(delegator_bid_information) => {
                let bids = delegator_bid_information.bids();
                BidQueryResult::new(bids.clone())
            }
        };

        Ok(Self::ResponseResult {
            api_version: CURRENT_API_VERSION,
            bid_records: resp,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Range, sync::Arc};

    use casper_binary_port::InformationRequest;
    use casper_types::{
        EraId, PublicKey,
        system::auction::{
            Bid, BidKind, Bridge, DelegatorBid, DelegatorKind, Reservation, ValidatorBid,
            ValidatorCredit,
        },
        testing::TestRng,
    };
    use rand::Rng;

    use crate::rpcs::{
        CURRENT_API_VERSION, RpcWithParams,
        action::query_bid::{QueryBid, QueryBids, QueryBidsParams, QueryBidsResult},
        common::BidQueryResult,
        test_utils::BinaryPortMock,
    };

    #[tokio::test]
    async fn should_return_when_asking_for_validator_bids() {
        let rng = &mut TestRng::new();
        let binary_port_mock = BinaryPortMock::new();

        let public_key = PublicKey::random(rng);
        let query_bid = QueryBid::ValidatorBid {
            public_key: Box::new(public_key.clone()),
        };
        let information_request = InformationRequest::ValidatorBid {
            state_identifier: None,
            public_key: Box::new(public_key),
        };
        let bids = random_bids(rng, 1..50);
        binary_port_mock
            .add_bids_request_response(information_request, bids.clone())
            .await;

        let resp = QueryBids::do_handle_request(
            Arc::new(binary_port_mock),
            QueryBidsParams {
                state_identifier: None,
                query: query_bid,
            },
        )
        .await
        .expect("should handle request");

        let bid_records = BidQueryResult::new(bids);
        let expected_response = QueryBidsResult {
            api_version: CURRENT_API_VERSION,
            bid_records,
        };

        assert_eq!(resp, expected_response);
    }

    #[tokio::test]
    async fn should_return_when_asking_for_delegator_bids() {
        let rng = &mut TestRng::new();
        let binary_port_mock = BinaryPortMock::new();

        let public_key = PublicKey::random(rng);
        let delegator_public_key = PublicKey::random(rng);
        let query_bid = QueryBid::DelegatorBid {
            validator_public_key: Box::new(public_key.clone()),
            delegator: Box::new(DelegatorKind::PublicKey(delegator_public_key.clone())),
        };
        let information_request = InformationRequest::DelegatorBid {
            state_identifier: None,
            validator_public_key: Box::new(public_key),
            delegator: Box::new(DelegatorKind::PublicKey(delegator_public_key)),
        };
        let bids = random_bids(rng, 1..50);
        binary_port_mock
            .add_bids_request_response(information_request, bids.clone())
            .await;

        let resp = QueryBids::do_handle_request(
            Arc::new(binary_port_mock),
            QueryBidsParams {
                state_identifier: None,
                query: query_bid,
            },
        )
        .await
        .expect("should handle request");

        let bid_records = BidQueryResult::new(bids);
        let expected_response = QueryBidsResult {
            api_version: CURRENT_API_VERSION,
            bid_records,
        };

        assert_eq!(resp, expected_response);
    }

    fn random_bids(rng: &mut TestRng, between: Range<u32>) -> Vec<BidKind> {
        let mut bids = vec![];

        for _ in between {
            bids.push(random_bid_kind(rng));
        }
        bids
    }

    fn random_bid_kind(rng: &mut TestRng) -> BidKind {
        match rng.gen_range(0..=5) {
            0 => {
                let random_pk = PublicKey::random(rng);
                BidKind::Unified(Box::new(Bid::random_for_public_key(rng, random_pk)))
            }
            1 => {
                let random_pk = PublicKey::random(rng);
                BidKind::Validator(Box::new(ValidatorBid::random_for_public_key(
                    rng, random_pk,
                )))
            }
            2 => {
                let validator_public_key = PublicKey::random(rng);
                let delegator_public_key = PublicKey::random(rng);
                let delegator_kind = DelegatorKind::PublicKey(delegator_public_key);
                BidKind::Delegator(Box::new(DelegatorBid::random_for_validator_and_delegator(
                    rng,
                    validator_public_key,
                    delegator_kind,
                )))
            }
            3 => {
                let old_key = PublicKey::random(rng);
                let new_key = PublicKey::random(rng);
                let era_id = EraId::random(rng);
                BidKind::Bridge(Box::new(Bridge::new(old_key, new_key, era_id)))
            }
            4 => {
                let pk = PublicKey::random(rng);
                let era_id = EraId::random(rng);
                let amount = rng.r#gen();
                BidKind::Credit(Box::new(ValidatorCredit::new(pk, era_id, amount)))
            }
            5 => {
                let validator_public_key = PublicKey::random(rng);
                let delegator_public_key = PublicKey::random(rng);
                let delegator_kind = DelegatorKind::PublicKey(delegator_public_key);
                BidKind::Reservation(Box::new(Reservation::new(
                    validator_public_key,
                    delegator_kind,
                    rng.r#gen(),
                )))
            }
            _ => unreachable!("Generated too big number"),
        }
    }
}
