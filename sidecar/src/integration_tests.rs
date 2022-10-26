use super::*;
use crate::testing::fake_event_stream::{spin_up_fake_event_stream, EventStreamScenario};
use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use serial_test::serial;
use std::time::Duration;

const TEST_CONFIG_PATH: &str = "config_test.toml";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
#[ignore]
async fn should_bind_to_fake_event_stream_and_shutdown_cleanly() {
    let test_config = ConfigWithCleanup::new(TEST_CONFIG_PATH);

    tokio::spawn(spin_up_fake_event_stream(
        test_config.config.connection.node_connections[0].sse_port,
        EventStreamScenario::Realistic,
        60,
    ));

    run(test_config.config.clone())
        .await
        .expect("Error running sidecar");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
#[ignore]
async fn should_allow_client_connection_to_sse() {
    let test_config = ConfigWithCleanup::new(TEST_CONFIG_PATH);

    tokio::spawn(spin_up_fake_event_stream(
        test_config.config.connection.node_connections[0].sse_port,
        EventStreamScenario::Realistic,
        60,
    ));

    tokio::spawn(run(test_config.config.clone()));

    // Allow sidecar to spin up
    tokio::time::sleep(Duration::from_secs(3)).await;

    let mut main_event_stream = reqwest::Client::new()
        .get("http://127.0.0.1:19999/events/main")
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
#[serial]
#[ignore]
async fn should_respond_to_rest_query() {
    let test_config = ConfigWithCleanup::new(TEST_CONFIG_PATH);

    tokio::spawn(spin_up_fake_event_stream(
        test_config.config.connection.node_connections[0].sse_port,
        EventStreamScenario::Realistic,
        60,
    ));

    tokio::spawn(run(test_config.config.clone()));

    // Allow sidecar to spin up
    tokio::time::sleep(Duration::from_secs(3)).await;

    let response = reqwest::Client::new()
        .get("http://127.0.0.1:17777/block")
        .send()
        .await
        .expect("Error requesting the /block endpoint");

    assert!(response.status().is_success());
}
