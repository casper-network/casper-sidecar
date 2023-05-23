use casper_event_types::FinalitySignature as FinSig;
use casper_types::{testing::TestRng, AsymmetricType, Timestamp};
use http::StatusCode;
use warp::test::request;

use super::filters;
use crate::{
    rest_server::requests::{ListDeploysRequest, Page},
    testing::fake_database::FakeDatabase,
    types::{database::DeployAggregate, sse_events::*},
};
use std::str;

// Path elements
const BLOCK: &str = "block";
const DEPLOY: &str = "deploy";
const DEPLOYS: &str = "deploys";
const FAULTS: &str = "faults";
const SIGNATURES: &str = "signatures";
const STEP: &str = "step";
const ACCEPTED: &str = "accepted";
const PROCESSED: &str = "processed";
const EXPIRED: &str = "expired";

// Example parameters
const VALID_HASH: &str = "0bcd71363b01c1c147c1603d2cc945930dcceecd869275beeee61dfc83b27a2c";
const VALID_ERA: u64 = 2304;
const VALID_PUBLIC_KEY: &str = "01a601840126a0363a6048bfcbb0492ab5a313a1a19dc4c695650d8f3b51302703";
const INVALID_HASH: &str = "not_a_hash";
const INVALID_PUBLIC_KEY: &str = "not_a_public_key";

async fn should_respond_to_path_with(request_path: String, expected_status: StatusCode) {
    let database = FakeDatabase::new();

    let api = filters::combined_filters(database);

    let response = request().path(&request_path).reply(&api).await;

    assert_eq!(response.status(), expected_status);
}

#[tokio::test]
async fn root_should_return_400() {
    should_respond_to_path_with("/".to_string(), StatusCode::BAD_REQUEST).await;
}

#[tokio::test]
async fn root_with_invalid_path_should_return_400() {
    should_respond_to_path_with("/not_block_or_deploy".to_string(), StatusCode::BAD_REQUEST).await;
    should_respond_to_path_with(
        "/not_block_or_deploy/extra".to_string(),
        StatusCode::BAD_REQUEST,
    )
    .await;
}

#[tokio::test]
async fn block_root_should_return_valid_data() {
    let database = FakeDatabase::new();

    // The database doesn't need to be populated with events for this test as it returns a random BlockAdded for get_latest_block()

    let api = filters::combined_filters(database);

    let request_path = format!("/{}", BLOCK);

    let response = request().path(&request_path).reply(&api).await;

    assert!(response.status().is_success());

    let body = response.into_body();
    serde_json::from_slice::<BlockAdded>(&body).expect("Error parsing BlockAdded from response");
}

#[tokio::test]
async fn block_by_hash_should_return_valid_data() {
    let database = FakeDatabase::new();

    let identifiers = database
        .populate_with_events()
        .await
        .expect("Error populating FakeDatabase");

    let api = filters::combined_filters(database);

    let request_path = format!("/{}/{}", BLOCK, identifiers.block_added_hash);

    let response = request().path(&request_path).reply(&api).await;

    assert!(response.status().is_success());

    let body = response.into_body();
    let block_added = serde_json::from_slice::<BlockAdded>(&body)
        .expect("Error parsing BlockAdded from response");

    assert_eq!(block_added.hex_encoded_hash(), identifiers.block_added_hash);
}

#[tokio::test]
async fn block_by_height_should_return_valid_data() {
    let database = FakeDatabase::new();

    let identifiers = database
        .populate_with_events()
        .await
        .expect("Error populating FakeDatabase");

    let api = filters::combined_filters(database);

    let request_path = format!("/{}/{}", BLOCK, identifiers.block_added_height);

    let response = request().path(&request_path).reply(&api).await;

    assert!(response.status().is_success());

    let body = response.into_body();
    let block_added = serde_json::from_slice::<BlockAdded>(&body)
        .expect("Error parsing BlockAdded from response");

    assert_eq!(block_added.get_height(), identifiers.block_added_height);
}

#[tokio::test]
async fn deploy_by_hash_should_return_valid_data() {
    let database = FakeDatabase::new();

    let identifiers = database
        .populate_with_events()
        .await
        .expect("Error populating FakeDatabase");

    let api = filters::combined_filters(database);

    let request_path = format!("/{}/{}", DEPLOY, identifiers.deploy_accepted_hash);

    let response = request().path(&request_path).reply(&api).await;

    assert!(response.status().is_success());

    let body = response.into_body();
    let deploy_aggregate = serde_json::from_slice::<DeployAggregate>(&body)
        .expect("Error parsing AggregateDeployInfo from response");

    assert_eq!(
        deploy_aggregate.deploy_hash,
        identifiers.deploy_accepted_hash
    );
}

#[tokio::test]
async fn deploy_accepted_by_hash_should_return_valid_data() {
    let database = FakeDatabase::new();

    let identifiers = database
        .populate_with_events()
        .await
        .expect("Error populating FakeDatabase");

    let api = filters::combined_filters(database);

    let request_path = format!(
        "/{}/{}/{}",
        DEPLOY, ACCEPTED, identifiers.deploy_accepted_hash
    );

    let response = request().path(&request_path).reply(&api).await;

    assert!(response.status().is_success());

    let body = response.into_body();
    let deploy_accepted = serde_json::from_slice::<DeployAccepted>(&body)
        .expect("Error parsing DeployAccepted from response");

    assert_eq!(
        deploy_accepted.hex_encoded_hash(),
        identifiers.deploy_accepted_hash
    );
}

#[tokio::test]
async fn deploy_processed_by_hash_should_return_valid_data() {
    let database = FakeDatabase::new();

    let identifiers = database
        .populate_with_events()
        .await
        .expect("Error populating FakeDatabase");

    let api = filters::combined_filters(database);

    let request_path = format!(
        "/{}/{}/{}",
        DEPLOY, PROCESSED, identifiers.deploy_processed_hash
    );

    let response = request().path(&request_path).reply(&api).await;

    assert!(response.status().is_success());

    let body = response.into_body();
    let deploy_processed = serde_json::from_slice::<DeployProcessed>(&body)
        .expect("Error parsing DeployProcessed from response");

    assert_eq!(
        deploy_processed.hex_encoded_hash(),
        identifiers.deploy_processed_hash
    );
}

#[tokio::test]
async fn deploy_expired_by_hash_should_return_valid_data() {
    let database = FakeDatabase::new();

    let identifiers = database
        .populate_with_events()
        .await
        .expect("Error populating FakeDatabase");

    let api = filters::combined_filters(database);

    let request_path = format!(
        "/{}/{}/{}",
        DEPLOY, EXPIRED, identifiers.deploy_expired_hash
    );

    let response = request().path(&request_path).reply(&api).await;

    assert!(response.status().is_success());

    let body = response.into_body();
    let deploy_expired = serde_json::from_slice::<DeployExpired>(&body)
        .expect("Error parsing DeployExpired from response");

    assert_eq!(
        deploy_expired.hex_encoded_hash(),
        identifiers.deploy_expired_hash
    );
}

#[tokio::test]
async fn step_by_era_should_return_valid_data() {
    let database = FakeDatabase::new();

    let identifiers = database
        .populate_with_events()
        .await
        .expect("Error populating FakeDatabase");

    let api = filters::combined_filters(database);

    let request_path = format!("/{}/{}", STEP, identifiers.step_era_id);

    let response = request().path(&request_path).reply(&api).await;

    assert!(response.status().is_success());

    let body = response.into_body();
    let step = serde_json::from_slice::<Step>(&body).expect("Error parsing Step from response");

    assert_eq!(step.era_id.value(), identifiers.step_era_id);
}

#[tokio::test]
async fn faults_by_public_key_should_return_valid_data() {
    let database = FakeDatabase::new();

    let identifiers = database
        .populate_with_events()
        .await
        .expect("Error populating FakeDatabase");

    let api = filters::combined_filters(database);

    let request_path = format!("/{}/{}", FAULTS, identifiers.fault_public_key);

    let response = request().path(&request_path).reply(&api).await;

    assert!(response.status().is_success());

    let body = response.into_body();
    let faults =
        serde_json::from_slice::<Vec<Fault>>(&body).expect("Error parsing Fault from response");

    assert_eq!(faults[0].public_key.to_hex(), identifiers.fault_public_key);
}

#[tokio::test]
async fn faults_by_era_should_return_valid_data() {
    let database = FakeDatabase::new();

    let identifiers = database
        .populate_with_events()
        .await
        .expect("Error populating FakeDatabase");

    let api = filters::combined_filters(database);

    let request_path = format!("/{}/{}", FAULTS, identifiers.fault_era_id);

    let response = request().path(&request_path).reply(&api).await;

    assert!(response.status().is_success());

    let body = response.into_body();
    let faults =
        serde_json::from_slice::<Vec<Fault>>(&body).expect("Error parsing Fault from response");

    assert_eq!(faults[0].era_id.value(), identifiers.fault_era_id);
}

#[tokio::test]
async fn finality_signatures_by_block_should_return_valid_data() {
    let database = FakeDatabase::new();

    let identifiers = database
        .populate_with_events()
        .await
        .expect("Error populating FakeDatabase");

    let api = filters::combined_filters(database);

    let request_path = format!(
        "/{}/{}",
        SIGNATURES, identifiers.finality_signatures_block_hash
    );

    let response = request().path(&request_path).reply(&api).await;

    assert!(response.status().is_success());

    let body = response.into_body();
    let finality_signatures = serde_json::from_slice::<Vec<FinSig>>(&body)
        .expect("Error parsing FinalitySignatures from response");

    assert_eq!(
        hex::encode(finality_signatures[0].block_hash().inner()),
        identifiers.finality_signatures_block_hash
    );
}

#[tokio::test]
async fn block_by_hash_of_not_stored_should_return_404() {
    let request_path = format!("/{}/{}", BLOCK, VALID_HASH);

    should_respond_to_path_with(request_path, StatusCode::NOT_FOUND).await
}

#[tokio::test]
async fn block_by_height_of_not_stored_should_return_404() {
    let request_path = format!("/{}/{}", BLOCK, 0);

    should_respond_to_path_with(request_path, StatusCode::NOT_FOUND).await
}

#[tokio::test]
async fn deploy_by_hash_of_not_stored_should_return_404() {
    let request_path = format!("/{}/{}", DEPLOY, VALID_HASH);

    should_respond_to_path_with(request_path, StatusCode::NOT_FOUND).await
}

#[tokio::test]
async fn deploy_accepted_by_hash_of_not_stored_should_return_404() {
    let request_path = format!("/{}/{}/{}", DEPLOY, ACCEPTED, VALID_HASH);

    should_respond_to_path_with(request_path, StatusCode::NOT_FOUND).await
}

#[tokio::test]
async fn deploy_processed_by_hash_of_not_stored_should_return_404() {
    let request_path = format!("/{}/{}/{}", DEPLOY, PROCESSED, VALID_HASH);

    should_respond_to_path_with(request_path, StatusCode::NOT_FOUND).await
}

#[tokio::test]
async fn deploy_expired_by_hash_of_not_stored_should_return_404() {
    let request_path = format!("/{}/{}/{}", DEPLOY, EXPIRED, VALID_HASH);

    should_respond_to_path_with(request_path, StatusCode::NOT_FOUND).await
}

#[tokio::test]
async fn faults_by_public_key_of_not_stored_should_return_404() {
    let request_path = format!("/{}/{}", FAULTS, VALID_PUBLIC_KEY);

    should_respond_to_path_with(request_path, StatusCode::NOT_FOUND).await
}

#[tokio::test]
async fn faults_by_era_of_not_stored_should_return_404() {
    let request_path = format!("/{}/{}", FAULTS, VALID_ERA);

    should_respond_to_path_with(request_path, StatusCode::NOT_FOUND).await
}

#[tokio::test]
async fn finality_signature_by_block_hash_of_not_stored_should_return_404() {
    let request_path = format!("/{}/{}", SIGNATURES, VALID_HASH);

    should_respond_to_path_with(request_path, StatusCode::NOT_FOUND).await
}

#[tokio::test]
async fn step_by_era_of_not_stored_should_return_404() {
    let request_path = format!("/{}/{}", STEP, VALID_ERA);

    should_respond_to_path_with(request_path, StatusCode::NOT_FOUND).await
}

#[tokio::test]
async fn block_by_invalid_hash_should_return_400() {
    let request_path = format!("/{}/{}", BLOCK, INVALID_HASH);

    should_respond_to_path_with(request_path, StatusCode::BAD_REQUEST).await
}

#[tokio::test]
async fn deploy_by_hash_of_invalid_should_return_400() {
    let request_path = format!("/{}/{}", DEPLOY, INVALID_HASH);

    should_respond_to_path_with(request_path, StatusCode::BAD_REQUEST).await
}

#[tokio::test]
async fn deploy_accepted_by_hash_of_invalid_should_return_400() {
    let request_path = format!("/{}/{}/{}", DEPLOY, ACCEPTED, INVALID_HASH);

    should_respond_to_path_with(request_path, StatusCode::BAD_REQUEST).await
}

#[tokio::test]
async fn deploy_processed_by_hash_of_invalid_should_return_400() {
    let request_path = format!("/{}/{}/{}", DEPLOY, PROCESSED, INVALID_HASH);

    should_respond_to_path_with(request_path, StatusCode::BAD_REQUEST).await
}

#[tokio::test]
async fn deploy_expired_by_hash_of_invalid_should_return_400() {
    let request_path = format!("/{}/{}/{}", DEPLOY, EXPIRED, INVALID_HASH);

    should_respond_to_path_with(request_path, StatusCode::BAD_REQUEST).await
}

#[tokio::test]
async fn faults_by_invalid_public_key_should_return_400() {
    let request_path = format!("/{}/{}", FAULTS, INVALID_PUBLIC_KEY);

    should_respond_to_path_with(request_path, StatusCode::BAD_REQUEST).await
}

#[tokio::test]
async fn finality_signature_by_invalid_block_hash_should_return_400() {
    let request_path = format!("/{}/{}", SIGNATURES, INVALID_HASH);

    should_respond_to_path_with(request_path, StatusCode::BAD_REQUEST).await
}

#[tokio::test]
async fn should_have_correct_content_type() {
    let database = FakeDatabase::new();

    let api = filters::combined_filters(database);

    let request_path = format!("/{}", BLOCK);

    let response = request().path(&request_path).reply(&api).await;

    assert_eq!(
        response
            .headers()
            .get("content-type")
            .expect("Error extracting 'content-type' from headers"),
        "application/json"
    );
}

#[tokio::test]
async fn given_list_deploy_returns_error_should_return_error_response() {
    let database = FakeDatabase::new();
    let api = filters::combined_filters(database);
    let request_path = format!("/{}?limit=5&offset=4", DEPLOYS);
    let response = request()
        .method("POST")
        .json(&build_list_deploys_request())
        .path(&request_path)
        .reply(&api)
        .await;

    assert!(!response.status().is_success());
    let body = response.into_body();
    let json_raw = str::from_utf8(&body).unwrap();
    assert_eq!(
        json_raw,
        "{\"code\":404,\"message\":\"Query returned no results\"}"
    );
}

#[tokio::test]
async fn list_deploy_should_return_paged_data() {
    let mut database = FakeDatabase::new();
    let mut rng = TestRng::new();
    let aggregate1 = random_deploy_aggregate(&mut rng);
    let aggregate2 = random_deploy_aggregate(&mut rng);
    let data_to_ret = (vec![aggregate1.clone(), aggregate2.clone()], 3);
    database.set_aggregates(data_to_ret);
    let api = filters::combined_filters(database);

    let request_path = format!("/{}", DEPLOYS);
    let response = request()
        .method("POST")
        .json(&build_list_deploys_request())
        .path(&request_path)
        .reply(&api)
        .await;

    assert!(response.status().is_success());

    let body = response.into_body();
    let page = serde_json::from_slice::<Page<DeployAggregate>>(&body)
        .expect("Error parsing Page from response");
    assert_eq!(page.offset, 4);
    assert_eq!(page.limit, 5);
    assert_eq!(page.item_count, 3);
    assert_eq!(page.data.len(), 2);
    assert_eq!(
        page.data.get(0).unwrap().deploy_hash,
        aggregate1.deploy_hash
    );
    assert_eq!(
        page.data.get(1).unwrap().deploy_hash,
        aggregate2.deploy_hash
    );
}

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

fn build_list_deploys_request() -> ListDeploysRequest {
    ListDeploysRequest {
        limit: Some(5),
        offset: Some(4),
        exclude_expired: None,
        exclude_not_processed: None,
        sort_column: None,
        sort_order: None,
    }
}
