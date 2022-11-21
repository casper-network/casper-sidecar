use bytes::Bytes;
use std::time::Duration;

use eventsource_stream::{EventStream, Eventsource};
use futures_util::StreamExt;
use http::StatusCode;
use tempfile::tempdir;

use casper_event_types::SseData;
use casper_types::testing::TestRng;
use futures::Stream;

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
    let test_rng = Box::leak(Box::new(TestRng::new()));

    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");
    let testing_config = prepare_config(&temp_storage_dir);

    let ess_config = EssConfig::new(testing_config.connection_port(), None, None);

    tokio::spawn(spin_up_fake_event_stream(
        test_rng,
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
    let test_rng = Box::leak(Box::new(TestRng::new()));

    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");
    let testing_config = prepare_config(&temp_storage_dir);

    let ess_config = EssConfig::new(testing_config.connection_port(), None, None);

    tokio::spawn(spin_up_fake_event_stream(
        test_rng,
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

    let mut main_event_stream = try_connect(&main_event_stream_url).await;

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
    let test_rng = Box::leak(Box::new(TestRng::new()));

    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");
    let testing_config = prepare_config(&temp_storage_dir).configure_retry_settings(0, 0);

    let ess_config = EssConfig::new(testing_config.connection_port(), None, None);

    tokio::spawn(spin_up_fake_event_stream(
        test_rng,
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

    let mut main_event_stream = try_connect(&main_event_stream_url).await;

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
    let test_rng = Box::leak(Box::new(TestRng::new()));

    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");
    let testing_config = prepare_config(&temp_storage_dir);

    let ess_config = EssConfig::new(testing_config.connection_port(), None, None);

    tokio::spawn(spin_up_fake_event_stream(
        test_rng,
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

    let mut main_event_stream = try_connect(&main_event_stream_url).await;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
// This waits to receive a BlockAdded to ensure that the listener has prioritised connecting to the main filter.
async fn should_allow_partial_connection_on_one_filter() {
    let did_receive_events = partial_connection_test(100, 1, true).await;
    assert!(did_receive_events)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
// This waits to receive a BlockAdded to ensure that the listener has prioritised connecting to the main filter.
async fn should_allow_partial_connection_on_two_filters() {
    let did_receive_events = partial_connection_test(100, 2, true).await;
    assert!(did_receive_events)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn should_disallow_partial_connection_on_one_filter() {
    let did_not_receive_events = !partial_connection_test(100, 1, false).await;
    assert!(did_not_receive_events)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn should_disallow_partial_connection_on_two_filters() {
    let did_not_receive_events = !partial_connection_test(100, 2, false).await;
    assert!(did_not_receive_events)
}

#[cfg(test)]
async fn partial_connection_test(
    num_of_events_to_send: u8,
    max_subscribers_for_fes: u32,
    allow_partial_connection: bool,
) -> bool {
    let test_rng = Box::leak(Box::new(TestRng::new()));

    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");
    let testing_config = prepare_config(&temp_storage_dir)
        .configure_retry_settings(1, 2)
        .set_allow_partial_connection(allow_partial_connection);

    let ess_config = EssConfig::new(
        testing_config.connection_port(),
        None,
        Some(max_subscribers_for_fes),
    );

    let stream_duration = Duration::from_secs(60);

    tokio::spawn(spin_up_fake_event_stream(
        test_rng,
        ess_config,
        EventStreamScenario::Counted(num_of_events_to_send),
        stream_duration,
    ));

    tokio::spawn(run(testing_config.inner()));

    let main_event_stream_url = format!(
        "http://127.0.0.1:{}/events/main",
        testing_config.event_stream_server_port()
    );

    let mut main_event_stream = try_connect(&main_event_stream_url).await;

    let mut got_events = false;

    while let Some(event) = main_event_stream.next().await {
        let data = event.unwrap().data;
        if let Ok(SseData::BlockAdded { .. }) = serde_json::from_str(&data) {
            got_events = true;
            // break;
        }
    }

    got_events
}

#[cfg(test)]
async fn try_connect(url: &str) -> EventStream<impl Stream<Item = reqwest::Result<Bytes>> + Sized> {
    let mut event_stream = None;
    for _ in 0..3 {
        let event_source = reqwest::Client::new()
            .get(url)
            .send()
            .await
            .map(|response| {
                event_stream = Some(response.bytes_stream().eventsource());
            });
        if event_source.is_ok() {
            break;
        } else {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    event_stream.expect("Unable to connect to stream")
}
