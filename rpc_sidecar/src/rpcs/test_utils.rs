use std::{collections::BTreeMap, convert::TryInto, sync::Arc};

use async_trait::async_trait;
use casper_binary_port::{
    BinaryRequest, BinaryResponse, BinaryResponseAndRequest, GetRequest,
    GlobalStateEntityQualifier, GlobalStateQueryResult, GlobalStateRequest, InformationRequest,
};
use casper_types::{
    addressable_entity::EntityKindTag,
    bytesrepr::ToBytes,
    system::auction::{Bid, BidKind, EraInfo, SEIGNIORAGE_RECIPIENTS_SNAPSHOT_KEY},
    AddressableEntityHash, BlockHeader, CLValue, GlobalStateIdentifier, Key, KeyTag,
    ProtocolVersion, SemVer, StoredValue,
};
use once_cell::sync::Lazy;
use tokio::sync::Mutex;

use crate::{ClientError, NodeClient};

pub(crate) static PROTOCOL_VERSION: Lazy<ProtocolVersion> =
    Lazy::new(|| ProtocolVersion::new(SemVer::new(2, 0, 0)));

pub(crate) struct BinaryPortMock {
    request_responses: Arc<Mutex<Vec<(BinaryRequest, BinaryResponseAndRequest)>>>,
}

impl BinaryPortMock {
    pub fn new() -> Self {
        Self {
            request_responses: Arc::new(Mutex::new(vec![])),
        }
    }

    pub async fn add_era_info_req_res(
        &mut self,
        era_info: EraInfo,
        state_identifier: Option<GlobalStateIdentifier>,
    ) {
        let req = GlobalStateRequest::new(
            state_identifier,
            GlobalStateEntityQualifier::Item {
                base_key: Key::EraSummary,
                path: vec![],
            },
        );
        let req = BinaryRequest::Get(GetRequest::State(Box::new(req)));
        let stored_value = StoredValue::EraInfo(era_info);
        let res = BinaryResponse::from_value(
            GlobalStateQueryResult::new(stored_value, vec![]),
            *PROTOCOL_VERSION,
        );
        self.when_then(req, res).await;
    }

    pub async fn add_block_header_req_res(
        &mut self,
        block_header: BlockHeader,
        information_request: InformationRequest,
    ) {
        let get_request = information_request
            .try_into()
            .expect("should create request");
        let req = BinaryRequest::Get(get_request);
        let res = BinaryResponse::from_option(Some(block_header), *PROTOCOL_VERSION);
        self.when_then(req, res).await;
    }

    pub async fn add_bid_kinds_fetch_res(
        &mut self,
        bid_kinds: Vec<BidKind>,
        state_identifier: Option<GlobalStateIdentifier>,
    ) {
        let req = GetRequest::State(Box::new(GlobalStateRequest::new(
            state_identifier,
            GlobalStateEntityQualifier::AllItems {
                key_tag: KeyTag::BidAddr,
            },
        )));
        let stored_values: Vec<StoredValue> =
            bid_kinds.into_iter().map(StoredValue::BidKind).collect();
        let res = BinaryResponse::from_value(stored_values, *PROTOCOL_VERSION);
        self.when_then(BinaryRequest::Get(req), res).await;
    }

    pub async fn add_bids_fetch_res(
        &mut self,
        bids: Vec<Bid>,
        state_identifier: Option<GlobalStateIdentifier>,
    ) {
        let req = GetRequest::State(Box::new(GlobalStateRequest::new(
            state_identifier,
            GlobalStateEntityQualifier::AllItems {
                key_tag: KeyTag::Bid,
            },
        )));

        let stored_values: Vec<StoredValue> = bids
            .into_iter()
            .map(|b| StoredValue::Bid(Box::new(b)))
            .collect();
        let res = BinaryResponse::from_value(stored_values, *PROTOCOL_VERSION);
        self.when_then(BinaryRequest::Get(req), res).await;
    }

    pub async fn add_system_registry(
        &mut self,
        state_identifier: Option<GlobalStateIdentifier>,
        registry: BTreeMap<String, AddressableEntityHash>,
    ) {
        let req = GetRequest::State(Box::new(GlobalStateRequest::new(
            state_identifier,
            GlobalStateEntityQualifier::Item {
                base_key: Key::SystemEntityRegistry,
                path: vec![],
            },
        )));
        let cl_value = CLValue::from_t(registry).unwrap();
        let stored_value = StoredValue::CLValue(cl_value);

        let res = BinaryResponse::from_value(
            GlobalStateQueryResult::new(stored_value, vec![]),
            *PROTOCOL_VERSION,
        );
        self.when_then(BinaryRequest::Get(req), res).await;
    }

    pub async fn add_seigniorage_snapshot_under_addressable_entity(
        &mut self,
        state_identifier: Option<GlobalStateIdentifier>,
        auction_hash: AddressableEntityHash,
        maybe_seigniorage_snapshot: Option<StoredValue>,
    ) {
        let base_key = Key::addressable_entity_key(EntityKindTag::System, auction_hash);
        let req = GetRequest::State(Box::new(GlobalStateRequest::new(
            state_identifier,
            GlobalStateEntityQualifier::Item {
                base_key,
                path: vec![SEIGNIORAGE_RECIPIENTS_SNAPSHOT_KEY.to_owned()],
            },
        )));
        let res = BinaryResponse::from_option(
            maybe_seigniorage_snapshot.map(|v| GlobalStateQueryResult::new(v, vec![])),
            *PROTOCOL_VERSION,
        );
        self.when_then(BinaryRequest::Get(req), res).await;
    }

    pub async fn add_seigniorage_snapshot_under_key_hash(
        &mut self,
        state_identifier: Option<GlobalStateIdentifier>,
        auction_hash: AddressableEntityHash,
        maybe_seigniorage_snapshot: Option<StoredValue>,
    ) {
        let base_key = Key::Hash(auction_hash.value());
        let req = GetRequest::State(Box::new(GlobalStateRequest::new(
            state_identifier,
            GlobalStateEntityQualifier::Item {
                base_key,
                path: vec![SEIGNIORAGE_RECIPIENTS_SNAPSHOT_KEY.to_owned()],
            },
        )));
        let res = BinaryResponse::from_option(
            maybe_seigniorage_snapshot.map(|v| GlobalStateQueryResult::new(v, vec![])),
            *PROTOCOL_VERSION,
        );
        self.when_then(BinaryRequest::Get(req), res).await;
    }

    pub async fn when_then(&self, when: BinaryRequest, then: BinaryResponse) {
        let payload = when.to_bytes().unwrap();
        let response_and_request = BinaryResponseAndRequest::new(then, &payload, 0);
        let mut guard = self.request_responses.lock().await;
        guard.push((when, response_and_request));
    }
}

#[async_trait]
impl NodeClient for BinaryPortMock {
    async fn send_request(
        &self,
        req: BinaryRequest,
    ) -> Result<BinaryResponseAndRequest, ClientError> {
        let mut guard = self.request_responses.lock().await;
        let (request, response) = guard.remove(0);
        if request != req {
            panic!(
                "Got unexpected request: {:?}. \n\n Expected {:?}",
                req, request
            )
        }
        Ok(response)
    }
}
