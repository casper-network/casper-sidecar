use super::requests::ListDeploysRequest;
use bytes::Bytes;
use http::Response;
use warp::{Filter, test::request, Reply};
use crate::{types::{
    database::{DeployAggregate, DatabaseWriter, DeployAggregateSortColumn},
    sse_events::{DeployAccepted, DeployProcessed, BlockAdded},
}, rest_server::requests::Page};
use casper_types::{Timestamp, testing::TestRng};

pub const DEPLOYS: &str = "deploys";

pub fn random_deploy_aggregate(rng: &mut casper_types::testing::TestRng) -> DeployAggregate {
    let deploy_accepted = DeployAccepted::random(rng);
    let deploy_processed = DeployProcessed::random(rng, Some(deploy_accepted.deploy_hash()));
    DeployAggregate {
        deploy_hash: deploy_accepted.deploy_hash().to_string(),
        deploy_accepted: Some(deploy_accepted),
        deploy_processed: Some(deploy_processed),
        deploy_expired: false,
        block_timestamp: Some(Timestamp::from(500)),
    }
}
pub fn build_list_deploys_request_limit_offset(limit: Option<u32>, offset: Option<u32>) -> ListDeploysRequest {
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

pub async fn populate_with_blocks_and_deploys(
    test_rng: &mut TestRng,
    database: &impl DatabaseWriter,
    number_of_blocks: usize,
    deploys_in_block: usize,
) -> (Vec<BlockAdded>, Vec<DeployAccepted>, Vec<DeployProcessed>) {
    let (blocks, accepteds, processeds) = generate_blocks_and_deploys(test_rng, number_of_blocks, deploys_in_block);
    let event_source_address = String::from("localhost");
    let mut event_id = 0;
    for block in blocks.iter() {
        database.save_block_added(block.clone(), event_id, event_source_address.clone()).await.unwrap();
        event_id += 1;
    }
    for accepted in accepteds.iter() {
        database.save_deploy_accepted(accepted.clone(), event_id, event_source_address.clone()).await.unwrap();
        event_id += 1;
    }
    for processed in processeds.iter() {
        database.save_deploy_processed(processed.clone(), event_id, event_source_address.clone()).await.unwrap();
        event_id += 1;
    }
    return (blocks, accepteds, processeds)
}

pub fn generate_blocks_and_deploys(
    test_rng: &mut TestRng,
    number_of_blocks: usize,
    deploys_in_block: usize,
) -> (Vec<BlockAdded>, Vec<DeployAccepted>, Vec<DeployProcessed>) {
    let mut blocks = Vec::new();
    let mut accepted_deploys = Vec::new();
    let mut processed_deploys = Vec::new();

    for block_number in 0..number_of_blocks {
        let mut deploy_hashes_for_block = Vec::new();
        for _ in 0..deploys_in_block {
            let accepted = DeployAccepted::random(test_rng);
            let processed = DeployProcessed::random(test_rng, Some(accepted.deploy_hash()));
            deploy_hashes_for_block.push(accepted.deploy_hash());
            accepted_deploys.push(accepted);
            processed_deploys.push(processed);
            
        }
        let block = BlockAdded::random_with_data(test_rng, deploy_hashes_for_block, block_number as u64);
        blocks.push(block);
    }
    (blocks, accepted_deploys, processed_deploys)
}

pub async fn list_deploys_raw<F>(api: &F, list_request_data: ListDeploysRequest) -> Response<Bytes> where 
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

pub fn deserialize_deploys(response :Response<Bytes>) -> Page<DeployAggregate> {
    let body = response.into_body();
    serde_json::from_slice::<Page<DeployAggregate>>(&body)
        .expect("Error parsing Page from response")

}
pub async fn list_deploys<F>(api: &F, list_request_data: ListDeploysRequest) -> Page<DeployAggregate> where 
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