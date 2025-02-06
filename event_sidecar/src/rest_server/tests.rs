use casper_types::{AsymmetricType, FinalitySignature as FinSig};
use http::StatusCode;
use warp::test::request;

use super::filters;
use crate::database::types::SseEnvelope;
use crate::{
    testing::fake_database::FakeDatabase,
    types::{database::TransactionAggregate, sse_events::*},
};

// Path elements
const BLOCK: &str = "block";
const TRANSACTION: &str = "transaction";
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
    should_respond_to_path_with(
        "/not_block_or_transaction".to_string(),
        StatusCode::BAD_REQUEST,
    )
    .await;
    should_respond_to_path_with(
        "/not_block_or_transaction/extra".to_string(),
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
    serde_json::from_slice::<SseEnvelope<BlockAdded>>(&body)
        .expect("Error parsing BlockAdded from response");
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
    let block_added = serde_json::from_slice::<SseEnvelope<BlockAdded>>(&body)
        .expect("Error parsing BlockAdded from response");

    assert_eq!(
        block_added.payload().hex_encoded_hash(),
        identifiers.block_added_hash
    );
    assert_eq!(block_added.network_name(), "network-1");
    assert_eq!(block_added.api_version(), "2.0.0");
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
    let block_added = serde_json::from_slice::<SseEnvelope<BlockAdded>>(&body)
        .expect("Error parsing BlockAdded from response");

    assert_eq!(
        block_added.payload().get_height(),
        identifiers.block_added_height
    );
    assert_eq!(block_added.network_name(), "network-1");
    assert_eq!(block_added.api_version(), "2.0.0");
}

#[tokio::test]
async fn transaction_by_hash_should_return_valid_data() {
    let database = FakeDatabase::new();

    let identifiers = database
        .populate_with_events()
        .await
        .expect("Error populating FakeDatabase");

    let api = filters::combined_filters(database);

    let (transaction_hash, transaction_type) = identifiers.transaction_accepted_info;
    let request_path = format!("/{}/{}/{}", TRANSACTION, transaction_type, transaction_hash);

    let response = request().path(&request_path).reply(&api).await;

    assert!(response.status().is_success());

    let body = response.into_body();
    let transaction_aggregate = serde_json::from_slice::<TransactionAggregate>(&body)
        .expect("Error parsing AggregateTransactionInfo from response");

    assert_eq!(transaction_aggregate.transaction_hash, transaction_hash);
}

#[tokio::test]
async fn transaction_accepted_by_hash_should_return_valid_data() {
    let database = FakeDatabase::new();

    let identifiers = database
        .populate_with_events()
        .await
        .expect("Error populating FakeDatabase");

    let api = filters::combined_filters(database);

    let (transaction_hash, transaction_type) = identifiers.transaction_accepted_info;
    let request_path = format!(
        "/{}/{}/{}/{}",
        TRANSACTION, ACCEPTED, transaction_type, transaction_hash
    );

    let response = request().path(&request_path).reply(&api).await;

    assert!(response.status().is_success());

    let body = response.into_body();
    let transaction_accepted = serde_json::from_slice::<SseEnvelope<TransactionAccepted>>(&body)
        .expect("Error parsing TransactionAccepted from response");

    assert_eq!(
        transaction_accepted.payload().hex_encoded_hash(),
        transaction_hash
    );
    assert_eq!(transaction_accepted.network_name(), "network-1");
    assert_eq!(transaction_accepted.api_version(), "2.0.0");
}

#[tokio::test]
async fn transaction_processed_by_hash_should_return_valid_data() {
    let database = FakeDatabase::new();

    let identifiers = database
        .populate_with_events()
        .await
        .expect("Error populating FakeDatabase");

    let api = filters::combined_filters(database);
    let (transaction_hash, transaction_type) = identifiers.transaction_processed_info;
    let request_path = format!(
        "/{}/{}/{}/{}",
        TRANSACTION, PROCESSED, transaction_type, transaction_hash
    );

    let response = request().path(&request_path).reply(&api).await;

    assert!(response.status().is_success());

    let body = response.into_body();
    let transaction_processed = serde_json::from_slice::<SseEnvelope<TransactionProcessed>>(&body)
        .expect("Error parsing TransactionProcessed from response");

    assert_eq!(
        transaction_processed.payload().hex_encoded_hash(),
        transaction_hash
    );
    assert_eq!(transaction_processed.network_name(), "network-1");
    assert_eq!(transaction_processed.api_version(), "2.0.0");
}

#[tokio::test]
async fn transaction_expired_by_hash_should_return_valid_data() {
    let database = FakeDatabase::new();

    let identifiers = database
        .populate_with_events()
        .await
        .expect("Error populating FakeDatabase");

    let api = filters::combined_filters(database);
    let (transaction_hash, transaction_type) = identifiers.transaction_expired_info;
    let request_path = format!(
        "/{}/{}/{}/{}",
        TRANSACTION, EXPIRED, transaction_type, transaction_hash
    );

    let response = request().path(&request_path).reply(&api).await;

    assert!(response.status().is_success());

    let body = response.into_body();
    let transaction_expired = serde_json::from_slice::<SseEnvelope<TransactionExpired>>(&body)
        .expect("Error parsing TransactionExpired from response");

    assert_eq!(
        transaction_expired.payload().hex_encoded_hash(),
        transaction_hash
    );
    assert_eq!(transaction_expired.network_name(), "network-1");
    assert_eq!(transaction_expired.api_version(), "2.0.0");
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
    let step = serde_json::from_slice::<SseEnvelope<Step>>(&body)
        .expect("Error parsing Step from response");

    assert_eq!(step.payload().era_id.value(), identifiers.step_era_id);
    assert_eq!(step.network_name(), "network-1");
    assert_eq!(step.api_version(), "2.0.0");
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
    let faults = serde_json::from_slice::<Vec<SseEnvelope<Fault>>>(&body)
        .expect("Error parsing Fault from response");

    let fault = &faults[0];
    assert_eq!(
        fault.payload().public_key.to_hex(),
        identifiers.fault_public_key
    );
    assert_eq!(fault.network_name(), "network-1");
    assert_eq!(fault.api_version(), "2.0.0");
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
    let faults = serde_json::from_slice::<Vec<SseEnvelope<Fault>>>(&body)
        .expect("Error parsing Fault from response");

    let fault = &faults[0];
    assert_eq!(fault.payload().era_id.value(), identifiers.fault_era_id);
    assert_eq!(fault.network_name(), "network-1");
    assert_eq!(fault.api_version(), "2.0.0");
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
    let finality_signatures = serde_json::from_slice::<Vec<SseEnvelope<FinSig>>>(&body)
        .expect("Error parsing FinalitySignatures from response");
    let finality_signature = &finality_signatures[0];
    assert_eq!(
        hex::encode(finality_signature.payload().block_hash().inner()),
        identifiers.finality_signatures_block_hash
    );
    assert_eq!(finality_signature.api_version(), "2.0.0");
    assert_eq!(finality_signature.network_name(), "network-1");
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
async fn transaction_by_hash_of_not_stored_should_return_404() {
    let request_path = format!("/{}/deploy/{}", TRANSACTION, VALID_HASH);

    should_respond_to_path_with(request_path, StatusCode::NOT_FOUND).await
}

#[tokio::test]
async fn transaction_accepted_by_hash_of_not_stored_should_return_404() {
    let request_path = format!("/{}/{}/version1/{}", TRANSACTION, ACCEPTED, VALID_HASH);
    should_respond_to_path_with(request_path, StatusCode::NOT_FOUND).await
}

#[tokio::test]
async fn transaction_processed_by_hash_of_not_stored_should_return_404() {
    let request_path = format!("/{}/{}/deploy/{}", TRANSACTION, PROCESSED, VALID_HASH);

    should_respond_to_path_with(request_path, StatusCode::NOT_FOUND).await
}

#[tokio::test]
async fn transaction_expired_by_hash_of_not_stored_should_return_404() {
    let request_path = format!("/{}/{}/deploy/{}", TRANSACTION, EXPIRED, VALID_HASH);

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
async fn transaction_by_hash_of_invalid_should_return_400() {
    let request_path = format!("/{}/{}", TRANSACTION, INVALID_HASH);

    should_respond_to_path_with(request_path, StatusCode::BAD_REQUEST).await
}

#[tokio::test]
async fn transaction_accepted_by_hash_of_invalid_should_return_400() {
    let request_path = format!("/{}/{}/deploy/{}", TRANSACTION, ACCEPTED, INVALID_HASH);

    should_respond_to_path_with(request_path, StatusCode::BAD_REQUEST).await
}

#[tokio::test]
async fn transaction_processed_by_hash_of_invalid_should_return_400() {
    let request_path = format!("/{}/{}/deploy/{}", TRANSACTION, PROCESSED, INVALID_HASH);

    should_respond_to_path_with(request_path, StatusCode::BAD_REQUEST).await
}

#[tokio::test]
async fn transaction_expired_by_hash_of_invalid_should_return_400() {
    let request_path = format!("/{}/{}/deploy/{}", TRANSACTION, EXPIRED, INVALID_HASH);

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
