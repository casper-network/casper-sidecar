use std::time::{Duration, Instant};

use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use tempfile::tempdir;

use super::run;
use crate::{
    testing::{
        fake_event_stream::{spin_up_fake_event_stream, EventStreamScenario},
        testing_config::prepare_config,
    },
    types::sse_events::BlockAdded,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn should_bind_to_fake_event_stream_and_shutdown_cleanly() {
    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");
    let testing_config = prepare_config(&temp_storage_dir);

    tokio::spawn(spin_up_fake_event_stream(
        testing_config.connection_port(),
        EventStreamScenario::Realistic,
        30,
    ));

    run(testing_config.inner())
        .await
        .expect("Error running sidecar");

    // Allows the event stream server to finish caching the index before tempdir tries to remove the dir
    tokio::time::sleep(Duration::from_secs(1)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn should_allow_client_connection_to_sse() {
    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");
    let testing_config = prepare_config(&temp_storage_dir);

    tokio::spawn(spin_up_fake_event_stream(
        testing_config.connection_port(),
        EventStreamScenario::Realistic,
        60,
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

    while let Some(event) = main_event_stream.next().await {
        event.expect("Error from event stream - event should have been OK");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn should_respond_to_rest_query() {
    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");
    let testing_config = prepare_config(&temp_storage_dir);

    tokio::spawn(spin_up_fake_event_stream(
        testing_config.connection_port(),
        EventStreamScenario::Realistic,
        60,
    ));

    tokio::spawn(run(testing_config.inner()));

    // Allow sidecar to spin up and receive a block from the stream
    tokio::time::sleep(Duration::from_secs(5)).await;

    let block_request_url = format!(
        "http://127.0.0.1:{}/block",
        testing_config.rest_server_port()
    );
    let response = reqwest::Client::new()
        .get(&block_request_url)
        .send()
        .await
        .expect("Error requesting the /block endpoint");

    assert!(response.status().is_success());

    let response_bytes = response
        .bytes()
        .await
        .expect("Should have got bytes from response");
    serde_json::from_slice::<BlockAdded>(&response_bytes)
        .expect("Should have parsed BlockAdded from bytes");
}
