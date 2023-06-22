use std::collections::HashSet;

use super::requests::ListDeploysRequest;
use crate::{
    rest_server::requests::Page,
    types::{
        database::{DatabaseWriter, DeployAggregate, DeployAggregateSortColumn},
        sse_events::{BlockAdded, DeployAccepted, DeployProcessed},
    },
};
use bytes::Bytes;
use casper_types::{testing::TestRng, Timestamp};
use http::Response;
use serde_json::value::to_raw_value;
use warp::{test::request, Filter, Reply};

pub const DEPLOYS: &str = "deploys";

pub fn random_deploy_aggregate(rng: &mut casper_types::testing::TestRng) -> DeployAggregate {
    let deploy_accepted = DeployAccepted::random(rng);
    let deploy_accepted_raw = to_raw_value(&deploy_accepted).unwrap();
    let deploy_processed = DeployProcessed::random(rng, Some(deploy_accepted.deploy_hash()), None);
    let deploy_processed_raw = to_raw_value(&deploy_processed).unwrap();
    DeployAggregate {
        deploy_hash: deploy_accepted.deploy_hash().to_string(),
        deploy_accepted: Some(deploy_accepted_raw),
        deploy_processed: Some(deploy_processed_raw),
        deploy_expired: false,
        block_timestamp: Some(Timestamp::from(500)),
    }
}

pub fn build_list_deploys_request_limit_offset(
    limit: Option<u32>,
    offset: Option<u32>,
) -> ListDeploysRequest {
    ListDeploysRequest {
        limit,
        offset,
        exclude_expired: None,
        exclude_not_processed: None,
        sort_column: Some(DeployAggregateSortColumn::BlockTimestamp),
        sort_order: Some(crate::types::database::SortOrder::Asc),
    }
}

pub fn build_list_deploys_request() -> ListDeploysRequest {
    build_list_deploys_request_limit_offset(None, None)
}

pub fn fetch_ids_from_events(
    vecs: (Vec<BlockAdded>, Vec<DeployAccepted>, Vec<DeployProcessed>),
) -> HashSet<String> {
    let mut all_deploy_hashes = HashSet::new();
    let (blocks, deploys_accepted, deploys_processed) = vecs;
    blocks
        .into_iter()
        .flat_map(|block| block.get_all_deploy_hashes())
        .for_each(|hash| {
            all_deploy_hashes.insert(hash);
        });
    deploys_accepted
        .into_iter()
        .map(|el| el.hex_encoded_hash())
        .for_each(|hash| {
            all_deploy_hashes.insert(hash);
        });
    deploys_processed
        .into_iter()
        .map(|el| el.hex_encoded_hash())
        .for_each(|hash| {
            all_deploy_hashes.insert(hash);
        });
    all_deploy_hashes
}

pub async fn populate_with_blocks_and_deploys(
    test_rng: &mut TestRng,
    database: &impl DatabaseWriter,
    number_of_blocks: u32,
    deploys_in_block: u32,
    id_base: Option<u32>,
) -> (Vec<BlockAdded>, Vec<DeployAccepted>, Vec<DeployProcessed>) {
    let base_offset = id_base.unwrap_or(0) as u64;
    let (blocks, accepteds, processeds) = generate_blocks_and_deploys(
        test_rng,
        number_of_blocks as usize,
        deploys_in_block as usize,
        base_offset,
    );
    let event_source_address = String::from("localhost");
    let mut event_id = id_base.unwrap_or(0);
    for accepted in accepteds.iter() {
        database
            .save_deploy_accepted(accepted.clone(), event_id, event_source_address.clone())
            .await
            .unwrap();
        event_id += 1;
    }
    for processed in processeds.iter() {
        database
            .save_deploy_processed(processed.clone(), event_id, event_source_address.clone())
            .await
            .unwrap();
        event_id += 1;
    }
    for block in blocks.iter() {
        database
            .save_block_added(block.clone(), event_id, event_source_address.clone())
            .await
            .unwrap();
        event_id += 1;
    }
    let mut count = 1;
    while count > 0 {
        count = database.update_pending_deploy_aggregates().await.unwrap()
    }
    (blocks, accepteds, processeds)
}

pub fn generate_blocks_and_deploys(
    test_rng: &mut TestRng,
    number_of_blocks: usize,
    deploys_in_block: usize,
    base_offset: u64,
) -> (Vec<BlockAdded>, Vec<DeployAccepted>, Vec<DeployProcessed>) {
    let mut blocks = Vec::new();
    let mut accepted_deploys = Vec::new();
    let mut processed_deploys = Vec::new();

    for block_number in 0..number_of_blocks {
        let mut deploy_hashes_for_block = Vec::new();
        for _ in 0..deploys_in_block {
            let accepted = DeployAccepted::random(test_rng);
            deploy_hashes_for_block.push(accepted.deploy_hash());
            accepted_deploys.push(accepted);
        }
        let block = BlockAdded::random_with_data(
            test_rng,
            deploy_hashes_for_block.clone(),
            base_offset + block_number as u64,
        );
        let block_hash = block.block.hash;
        blocks.push(block);
        for i in 0..deploys_in_block {
            let deploy_hash = deploy_hashes_for_block.get(i).unwrap();
            let processed = DeployProcessed::random(test_rng, Some(*deploy_hash), Some(block_hash));
            processed_deploys.push(processed);
        }
    }
    (blocks, accepted_deploys, processed_deploys)
}

pub async fn list_deploys<F>(
    api: &F,
    list_request_data: ListDeploysRequest,
) -> Page<DeployAggregate>
where
    F: Filter + 'static,
    F::Extract: Reply + Send,
{
    let request_path = format!("/{}", DEPLOYS);
    let response = request()
        .method("POST")
        .json(&list_request_data)
        .path(&request_path)
        .reply(api)
        .await;

    assert!(response.status().is_success());

    let body = response.into_body();
    serde_json::from_slice::<Page<DeployAggregate>>(&body)
        .expect("Error parsing Page from response")
}

pub async fn list_deploys_raw<F>(api: &F, list_request_data: ListDeploysRequest) -> Response<Bytes>
where
    F: Filter + 'static,
    F::Extract: Reply + Send,
{
    let request_path = format!("/{}", DEPLOYS);
    let response = request()
        .method("POST")
        .json(&list_request_data)
        .path(&request_path)
        .reply(api)
        .await;
    assert!(response.status().is_success());
    response
}

pub fn deserialize_deploys(response: Response<Bytes>) -> Page<DeployAggregate> {
    let body = response.into_body();
    serde_json::from_slice::<Page<DeployAggregate>>(&body)
        .expect("Error parsing Page from response")
}
