use casper_types::testing::TestRng;
use std::time::Duration;

use casper_event_types::SseData;
use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use http::StatusCode;
use rand::Rng;
use tempfile::tempdir;

use super::run;
use crate::{
    event_stream_server::Config as EssConfig,
    testing::{
        fake_event_stream::{spin_up_fake_event_stream, EventStreamScenario},
        testing_config::prepare_config,
    },
    types::sse_events::BlockAdded,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn should_bind_to_fake_event_stream_and_shutdown_cleanly() {
    let mut test_rng = TestRng::new();
    let rng_seed = test_rng.gen::<[u8; 16]>();

    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");
    let testing_config = prepare_config(&temp_storage_dir);

    let ess_config = EssConfig::new(testing_config.connection_port(), None, None);

    tokio::spawn(spin_up_fake_event_stream(
        rng_seed,
        ess_config,
        EventStreamScenario::Realistic,
        Duration::from_secs(30),
    ));

    let shutdown_err = run(testing_config.inner())
        .await
        .expect_err("Sidecar should return an Err message on shutdown");

    assert_eq!(
        shutdown_err.to_string(),
        "Connected node(s) are unavailable"
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn should_allow_client_connection_to_sse() {
    let mut test_rng = TestRng::new();
    let rng_seed = test_rng.gen::<[u8; 16]>();

    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");
    let testing_config = prepare_config(&temp_storage_dir);

    let ess_config = EssConfig::new(testing_config.connection_port(), None, None);

    tokio::spawn(spin_up_fake_event_stream(
        rng_seed,
        ess_config,
        EventStreamScenario::Realistic,
        Duration::from_secs(60),
    ));

    tokio::spawn(run(testing_config.inner()));

    // Allow sidecar to spin up
    tokio::time::sleep(Duration::from_secs(3)).await;

    let main_event_stream_url = format!(
        "http://127.0.0.1:{}/events/main",
        testing_config.event_stream_server_port()
    );
    let mut main_event_stream = reqwest::Client::new()
        .get(&main_event_stream_url)
        .send()
        .await
        .expect("Error in main event stream")
        .bytes_stream()
        .eventsource();

    let mut events_received = Vec::new();

    while let Some(Ok(event)) = main_event_stream.next().await {
        let sse_data = serde_json::from_str::<SseData>(&event.data).unwrap();
        events_received.push(sse_data);
    }

    assert!(!events_received.is_empty());
    assert!(events_received.len() > 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn should_send_shutdown_to_sse_client() {
    let mut test_rng = TestRng::new();
    let rng_seed = test_rng.gen::<[u8; 16]>();

    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");
    let testing_config = prepare_config(&temp_storage_dir).configure_retry_settings(0, 0);

    let ess_config = EssConfig::new(testing_config.connection_port(), None, None);

    tokio::spawn(spin_up_fake_event_stream(
        rng_seed,
        ess_config,
        EventStreamScenario::Realistic,
        Duration::from_secs(60),
    ));

    tokio::spawn(run(testing_config.inner()));

    // Allow sidecar to spin up
    tokio::time::sleep(Duration::from_secs(3)).await;

    let main_event_stream_url = format!(
        "http://127.0.0.1:{}/events/main",
        testing_config.event_stream_server_port()
    );
    let mut main_event_stream = reqwest::Client::new()
        .get(&main_event_stream_url)
        .send()
        .await
        .expect("Error in main event stream")
        .bytes_stream()
        .eventsource();

    let mut last_event = None;
    while let Some(Ok(event)) = main_event_stream.next().await {
        last_event = Some(event);
    }

    let event_data = last_event.unwrap().data;

    assert!(matches!(
        serde_json::from_str::<SseData>(&event_data).unwrap(),
        SseData::Shutdown
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn should_respond_to_rest_query() {
    let mut test_rng = TestRng::new();
    let rng_seed = test_rng.gen::<[u8; 16]>();

    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");
    let testing_config = prepare_config(&temp_storage_dir);

    let ess_config = EssConfig::new(testing_config.connection_port(), None, None);

    tokio::spawn(spin_up_fake_event_stream(
        rng_seed,
        ess_config,
        EventStreamScenario::Realistic,
        Duration::from_secs(60),
    ));

    tokio::spawn(run(testing_config.inner()));

    tokio::time::sleep(Duration::from_secs(1)).await;

    let main_event_stream_url = format!(
        "http://127.0.0.1:{}/events/main",
        testing_config.event_stream_server_port()
    );

    let mut main_event_stream = reqwest::Client::new()
        .get(&main_event_stream_url)
        .send()
        .await
        .expect("Error in main event stream")
        .bytes_stream()
        .eventsource();

    // Listen to the sidecar's outbound stream to check for it storing a BlockAdded
    while let Some(Ok(event)) = main_event_stream.next().await {
        let sse_data = serde_json::from_str::<SseData>(&event.data).unwrap();
        if matches!(sse_data, SseData::BlockAdded { .. }) {
            break;
        }
    }

    let block_request_url = format!(
        "http://127.0.0.1:{}/block",
        testing_config.rest_server_port()
    );
    let response = reqwest::Client::new()
        .get(&block_request_url)
        .send()
        .await
        .expect("Error requesting the /block endpoint");

    assert!(response.status() == StatusCode::OK || response.status() == StatusCode::NOT_FOUND);

    let response_bytes = response
        .bytes()
        .await
        .expect("Should have got bytes from response");
    serde_json::from_slice::<BlockAdded>(&response_bytes)
        .expect("Should have parsed BlockAdded from bytes");
}
