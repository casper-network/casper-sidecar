use async_trait::async_trait;
use casper_json_rpc::Error as RpcError;
use casper_types::{
    AccessRights, BlockV2, GlobalStateIdentifier, PublicKey, SecretKey, URef, URefAddr,
    system::auction::{DelegatorKind, ValidatorBid},
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
    ValidatorBid {
        public_key: Box<PublicKey>,
        include_delegators: bool,
    },
    DelegatorBid {
        validator_public_key: Box<PublicKey>,
        delegator: Box<DelegatorKind>,
    },
}

/// Parameters for "state_get_trie" RPC request.
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
        query: QueryBid::ValidatorBid {
            public_key,
            include_delegators: true,
        },
    }
});

static GET_QUERY_BIDS_RESULT: LazyLock<QueryBidsResult> = LazyLock::new(|| {
    let secret_key = SecretKey::ed25519_from_bytes([0; 32]).unwrap();
    let public_key = PublicKey::from(&secret_key);
    let uref = URef::new([250; 32], AccessRights::READ_ADD_WRITE);
    let bid_record = BidQueryResult::Validator {
        validator: ValidatorBid::empty(public_key, uref),
        delegators: vec![],
    };
    QueryBidsResult {
        api_version: DOCS_EXAMPLE_API_VERSION,
        bid_record,
    }
});

impl DocExample for QueryBidsParams {
    fn doc_example() -> &'static Self {
        &GET_QUERY_BIDS_PARAMS
    }
}

/// Result for "state_get_trie" RPC response.
#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QueryBidsResult {
    /// The RPC API version.
    #[schemars(with = "String")]
    pub api_version: ApiVersion,
    /// Bid information.
    pub bid_record: BidQueryResult,
}

impl DocExample for QueryBidsResult {
    fn doc_example() -> &'static Self {
        &GET_QUERY_BIDS_RESULT
    }
}

/// `state_query_bids` RPC.
pub struct QueryBids {}

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
                let (validator, delegators) = validator_bid_information.destructure();
                BidQueryResult::Validator {
                    validator,
                    delegators,
                }
            }
            BidQueryResponse::Delegator(delegator_bid_information) => {
                let (validator, delegator) = delegator_bid_information.destructure();
                BidQueryResult::Delegator {
                    validator,
                    delegator,
                }
            }
        };

        Ok(Self::ResponseResult {
            api_version: CURRENT_API_VERSION,
            bid_record: resp,
        })
    }
}

#[test]
fn x() {
    let secret_key = SecretKey::from_file(
        "/home/zajko/DEV/src/CA/casper-nctl/assets/net-1/nodes/node-1/keys/secret_key.pem",
    )
    .unwrap();
    let z = PublicKey::from(&secret_key);

    println!("A {}", serde_json::to_string(&z).unwrap());
}
