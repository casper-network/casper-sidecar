use http::StatusCode;
use warp::test::request;

use casper_node::types::FinalitySignature as FinSig;

use super::filters;
use crate::{
    testing::mock_database::MockDatabase,
    types::{database::DeployAggregate, sse_events::*},
};
//
// struct IdentifiersForStoredEvents {
//     block_added_hash: String,
//     block_added_height: u64,
//     deploy_accepted_hash: String,
//     deploy_processed_hash: String,
//     deploy_expired_hash: String,
//     fault_public_key: String,
//     fault_era_id: u64,
//     finality_signatures_block_hash: String,
//     step_era_id: u64,
// }

const VALID_HASH: &str = "0bcd71363b01c1c147c1603d2cc945930dcceecd869275beeee61dfc83b27a2c";
const VALID_ERA: u64 = 2304;
const VALID_PUBLIC_KEY: &str = "01a601840126a0363a6048bfcbb0492ab5a313a1a19dc4c695650d8f3b51302703";
const INVALID_HASH: &str = "not_a_hash";
const INVALID_PUBLIC_KEY: &str = "not_a_public_key";

// Creates random SSE event data and saves them to the given SqliteDatabase returning the identifiers for each record.
// async fn populate_test_db_returning_keys(
//     db: &SqliteDatabase,
// ) -> Result<IdentifiersForStoredEvents, Error> {
//     let mut rng = TestRng::new();
//
//     let block_added = BlockAdded::random(&mut rng);
//     let deploy_accepted = DeployAccepted::random(&mut rng);
//     let deploy_processed = DeployProcessed::random(&mut rng, None);
//     let deploy_expired = DeployExpired::random(&mut rng, None);
//     let fault = Fault::random(&mut rng);
//     let finality_signature = FinalitySignature::random(&mut rng);
//     let step = Step::random(&mut rng);
//
//     let test_stored_keys = IdentifiersForStoredEvents {
//         block_added_hash: block_added.hex_encoded_hash(),
//         block_added_height: block_added.get_height(),
//         deploy_accepted_hash: deploy_accepted.hex_encoded_hash(),
//         deploy_processed_hash: deploy_processed.hex_encoded_hash(),
//         deploy_expired_hash: deploy_expired.hex_encoded_hash(),
//         fault_era_id: fault.era_id.value(),
//         fault_public_key: fault.public_key.to_hex(),
//         finality_signatures_block_hash: finality_signature.hex_encoded_block_hash(),
//         step_era_id: step.era_id.value(),
//     };
//
//     db.save_block_added(block_added, rng.gen(), "127.0.0.1".to_string())
//         .await?;
//     db.save_deploy_accepted(deploy_accepted, rng.gen(), "127.0.0.1".to_string())
//         .await?;
//     db.save_deploy_processed(deploy_processed, rng.gen(), "127.0.0.1".to_string())
//         .await?;
//     db.save_deploy_expired(deploy_expired, rng.gen(), "127.0.0.1".to_string())
//         .await?;
//     db.save_fault(fault, rng.gen(), "127.0.0.1".to_string())
//         .await?;
//     db.save_finality_signature(finality_signature, rng.gen(), "127.0.0.1".to_string())
//         .await?;
//     db.save_step(step, rng.gen(), "127.0.0.1".to_string())
//         .await?;
//
//     Ok(test_stored_keys)
// }

async fn should_respond_to_path_with(request_path: String, expected_status: StatusCode) {
    let database = MockDatabase::new();

    let api = filters::combined_filters(database);

    let response = request().path(&request_path).reply(&api).await;

    assert_eq!(response.status(), expected_status);
}

// async fn prepare_database() -> (SqliteDatabase, IdentifiersForStoredEvents) {
//     let db = SqliteDatabase::new_in_memory(MAX_CONNECTIONS)
//         .await
//         .expect("Error opening database in memory");
//
//     let stored_identifiers = populate_test_db_returning_keys(&db)
//         .await
//         .expect("Error populating database");
//
//     (db, stored_identifiers)
// }

#[tokio::test]
async fn root_should_return_400() {
    should_respond_to_path_with("/".to_string(), StatusCode::BAD_REQUEST).await;
}

#[tokio::test]
async fn root_with_invalid_path_should_return_400() {
    should_respond_to_path_with("/not_block_or_deploy".to_string(), StatusCode::BAD_REQUEST).await;
}

#[tokio::test]
async fn block_root_should_return_valid_data() {
    let database = MockDatabase::new();

    let api = filters::combined_filters(database);

    let response = request().path("/block").reply(&api).await;

    assert!(response.status().is_success());

    let body = response.into_body();
    serde_json::from_slice::<BlockAdded>(&body).expect("Error parsing BlockAdded from response");
}

#[tokio::test]
async fn block_by_hash_should_return_valid_data() {
    let database = MockDatabase::new();

    let api = filters::combined_filters(database);

    let request_path = format!("/block/{}", VALID_HASH);

    let response = request().path(&request_path).reply(&api).await;

    assert!(response.status().is_success());

    let body = response.into_body();
    serde_json::from_slice::<BlockAdded>(&body).expect("Error parsing BlockAdded from response");
}

#[tokio::test]
async fn block_by_height_should_return_valid_data() {
    let database = MockDatabase::new();

    let api = filters::combined_filters(database);

    let request_path = format!("/block/{}", VALID_ERA);

    let response = request().path(&request_path).reply(&api).await;

    assert!(response.status().is_success());

    let body = response.into_body();
    serde_json::from_slice::<BlockAdded>(&body).expect("Error parsing BlockAdded from response");
}

#[tokio::test]
async fn deploy_root_should_return_valid_data() {
    let database = MockDatabase::new();

    let api = filters::combined_filters(database);

    let response = request().path("/deploy").reply(&api).await;

    assert!(response.status().is_success());

    let body = response.into_body();
    serde_json::from_slice::<DeployAggregate>(&body)
        .expect("Error parsing AggregateDeployInfo from response");
}

#[tokio::test]
async fn deploy_by_hash_should_return_valid_data() {
    let database = MockDatabase::new();

    let api = filters::combined_filters(database);

    let request_path = format!("/deploy/{}", VALID_HASH);

    let response = request().path(&request_path).reply(&api).await;

    assert!(response.status().is_success());

    let body = response.into_body();
    serde_json::from_slice::<DeployAggregate>(&body)
        .expect("Error parsing AggregateDeployInfo from response");
}

#[tokio::test]
async fn deploy_accepted_by_hash_should_return_valid_data() {
    let database = MockDatabase::new();

    let api = filters::combined_filters(database);

    let request_path = format!("/deploy/accepted/{}", VALID_HASH);

    let response = request().path(&request_path).reply(&api).await;

    assert!(response.status().is_success());

    let body = response.into_body();
    serde_json::from_slice::<DeployAccepted>(&body)
        .expect("Error parsing DeployAccepted from response");
}

#[tokio::test]
async fn deploy_processed_by_hash_should_return_valid_data() {
    let database = MockDatabase::new();

    let api = filters::combined_filters(database);

    let request_path = format!("/deploy/processed/{}", VALID_HASH);

    let response = request().path(&request_path).reply(&api).await;

    assert!(response.status().is_success());

    let body = response.into_body();
    serde_json::from_slice::<DeployProcessed>(&body)
        .expect("Error parsing DeployProcessed from response");
}

#[tokio::test]
async fn deploy_expired_by_hash_should_return_valid_data() {
    let database = MockDatabase::new();

    let api = filters::combined_filters(database);

    let request_path = format!("/deploy/expired/{}", VALID_HASH);

    let response = request().path(&request_path).reply(&api).await;

    assert!(response.status().is_success());

    let body = response.into_body();
    let deploy_expired =
        serde_json::from_slice::<bool>(&body).expect("Error parsing DeployExpired from response");

    assert!(deploy_expired);
}

#[tokio::test]
async fn step_by_era_should_return_valid_data() {
    let database = MockDatabase::new();

    let api = filters::combined_filters(database);

    let request_path = format!("/step/{}", VALID_ERA);

    let response = request().path(&request_path).reply(&api).await;

    assert!(response.status().is_success());

    let body = response.into_body();
    serde_json::from_slice::<Step>(&body).expect("Error parsing Step from response");
}

#[tokio::test]
async fn faults_by_public_key_should_return_valid_data() {
    let database = MockDatabase::new();

    let api = filters::combined_filters(database);

    let request_path = format!("/faults/{}", VALID_PUBLIC_KEY);

    let response = request().path(&request_path).reply(&api).await;

    assert!(response.status().is_success());

    let body = response.into_body();
    serde_json::from_slice::<Vec<Fault>>(&body).expect("Error parsing Fault from response");
}

#[tokio::test]
async fn faults_by_era_should_return_valid_data() {
    let database = MockDatabase::new();

    let api = filters::combined_filters(database);

    let request_path = format!("/faults/{}", VALID_ERA);

    let response = request().path(&request_path).reply(&api).await;

    assert!(response.status().is_success());

    let body = response.into_body();
    serde_json::from_slice::<Vec<Fault>>(&body).expect("Error parsing Fault from response");
}

#[tokio::test]
async fn finality_signatures_by_block_should_return_valid_data() {
    let database = MockDatabase::new();

    let api = filters::combined_filters(database);

    let request_path = format!("/signatures/{}", VALID_HASH);

    let response = request().path(&request_path).reply(&api).await;

    assert!(response.status().is_success());

    let body = response.into_body();
    serde_json::from_slice::<Vec<FinSig>>(&body)
        .expect("Error parsing FinalitySignatures from response");
}

#[tokio::test]
async fn block_by_invalid_hash_should_return_400() {
    let request_path = format!("/block/{}", INVALID_HASH);

    should_respond_to_path_with(request_path, StatusCode::BAD_REQUEST).await
}

#[tokio::test]
async fn deploy_by_hash_of_invalid_should_return_400() {
    let request_path = format!("/deploy/{}", INVALID_HASH);

    should_respond_to_path_with(request_path, StatusCode::BAD_REQUEST).await
}

#[tokio::test]
async fn deploy_accepted_by_hash_of_invalid_should_return_400() {
    let request_path = format!("/deploy/accepted/{}", INVALID_HASH);

    should_respond_to_path_with(request_path, StatusCode::BAD_REQUEST).await
}

#[tokio::test]
async fn deploy_processed_by_hash_of_invalid_should_return_400() {
    let request_path = format!("/deploy/processed/{}", INVALID_HASH);

    should_respond_to_path_with(request_path, StatusCode::BAD_REQUEST).await
}

#[tokio::test]
async fn deploy_expired_by_hash_of_invalid_should_return_400() {
    let request_path = format!("/deploy/expired/{}", INVALID_HASH);

    should_respond_to_path_with(request_path, StatusCode::BAD_REQUEST).await
}

#[tokio::test]
async fn fault_by_invalid_public_key_should_return_400() {
    let request_path = format!("/faults/{}", INVALID_PUBLIC_KEY);

    should_respond_to_path_with(request_path, StatusCode::BAD_REQUEST).await
}

#[tokio::test]
async fn finality_signature_by_invalid_block_hash_should_return_400() {
    let request_path = format!("/signatures/{}", INVALID_HASH);

    should_respond_to_path_with(request_path, StatusCode::BAD_REQUEST).await
}

#[tokio::test]
async fn should_have_correct_content_type() {
    let database = MockDatabase::new();

    let api = filters::combined_filters(database);

    let response = request().path("/block").reply(&api).await;

    assert_eq!(
        response
            .headers()
            .get("content-type")
            .expect("Error extracting 'content-type' from headers"),
        "application/json"
    );
}
