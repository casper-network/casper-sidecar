use bytes::Bytes;
use std::time::Duration;

use eventsource_stream::{EventStream, Eventsource};
use futures_util::StreamExt;
use http::StatusCode;
use tempfile::tempdir;
use tokio::time::Instant;

use casper_event_listener::{EventListener, FilterPriority};
use casper_event_types::SseData;
use casper_types::testing::TestRng;
use futures::Stream;

use super::run;
use crate::performance_tests::EventType;
use crate::testing::fake_event_stream::{Bound, GenericScenarioSettings, Restart};
use crate::utils::display_duration;
use crate::{
    event_stream_server::Config as EssConfig,
    testing::{
        fake_event_stream::{spin_up_fake_event_stream, Scenario},
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
        Scenario::Realistic(GenericScenarioSettings::new(
            Bound::Timed(Duration::from_secs(30)),
            None,
        )),
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
        Scenario::Realistic(GenericScenarioSettings::new(
            Bound::Timed(Duration::from_secs(60)),
            None,
        )),
    ));

    tokio::spawn(run(testing_config.inner()));

    let main_event_stream_url = format!(
        "http://127.0.0.1:{}/events/main",
        testing_config.event_stream_server_port()
    );

    let mut main_event_stream = try_connect_to_single_stream(&main_event_stream_url).await;

    let mut events_received = Vec::new();

    while let Some(Ok(event)) = main_event_stream.next().await {
        let sse_data = serde_json::from_str::<SseData>(&event.data).unwrap();
        events_received.push(sse_data);
    }

    assert!(!events_received.is_empty());
    assert!(events_received.len() > 1);
    assert!(
        matches!(events_received[0], SseData::ApiVersion(_)),
        "First event should be ApiVersion"
    );
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
        Scenario::Realistic(GenericScenarioSettings::new(
            Bound::Timed(Duration::from_secs(60)),
            None,
        )),
    ));

    tokio::spawn(run(testing_config.inner()));

    let main_event_stream_url = format!(
        "http://127.0.0.1:{}/events/main",
        testing_config.event_stream_server_port()
    );

    let mut main_event_stream = try_connect_to_single_stream(&main_event_stream_url).await;

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
        Scenario::Realistic(GenericScenarioSettings::new(
            Bound::Timed(Duration::from_secs(60)),
            None,
        )),
    ));

    tokio::spawn(run(testing_config.inner()));

    let main_event_stream_url = format!(
        "http://127.0.0.1:{}/events/main",
        testing_config.event_stream_server_port()
    );

    let mut main_event_stream = try_connect_to_single_stream(&main_event_stream_url).await;

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
async fn should_allow_partial_connection_on_one_filter() {
    let received_event_types = partial_connection_test(100, 1, true, None).await;
    assert!(received_event_types.is_some())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn should_allow_partial_connection_on_two_filters() {
    let received_event_types = partial_connection_test(100, 2, true, None).await;
    assert!(received_event_types.is_some())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn should_disallow_partial_connection_on_one_filter() {
    let received_event_types = partial_connection_test(100, 1, false, None).await;
    assert!(received_event_types.is_none())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn should_disallow_partial_connection_on_two_filters() {
    let received_event_types = partial_connection_test(100, 2, false, None).await;
    assert!(received_event_types.is_none())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn should_prioritise_filters_correctly_main_only() {
    let filter_priority = FilterPriority::new(0, 1, 2).unwrap();
    let received_event_types = partial_connection_test(100, 1, true, Some(filter_priority)).await;
    assert!(received_event_types.is_some());

    if let Some(event_types) = received_event_types {
        // If any of the events received aren't part of the Main filter then it hasn't connected as configured
        assert!(!event_types.iter().any(|evt_type| {
            match evt_type {
                EventType::ApiVersion
                | EventType::BlockAdded
                | EventType::DeployExpired
                | EventType::DeployProcessed
                | EventType::Fault
                | EventType::Step
                | EventType::Shutdown => false,
                EventType::DeployAccepted | EventType::FinalitySignature => true,
            }
        }))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn should_prioritise_filters_correctly_sigs_only() {
    let filter_priority = FilterPriority::new(1, 0, 2).unwrap();
    let received_event_types = partial_connection_test(100, 1, true, Some(filter_priority)).await;
    assert!(received_event_types.is_some());

    if let Some(event_types) = received_event_types {
        // If any of the events received aren't part of the Sigs filter then it hasn't connected as configured
        assert!(!event_types.iter().any(|evt_type| {
            match evt_type {
                EventType::BlockAdded
                | EventType::DeployAccepted
                | EventType::DeployExpired
                | EventType::DeployProcessed
                | EventType::Fault
                | EventType::Step => true,
                EventType::ApiVersion | EventType::FinalitySignature | EventType::Shutdown => false,
            }
        }))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn should_prioritise_filters_correctly_deploys_only() {
    let filter_priority = FilterPriority::new(1, 2, 0).unwrap();
    let received_event_types = partial_connection_test(100, 1, true, Some(filter_priority)).await;
    assert!(received_event_types.is_some());

    if let Some(event_types) = received_event_types {
        // If any of the events received aren't part of the Deploys filter then it hasn't connected as configured
        assert!(!event_types.iter().any(|evt_type| {
            match evt_type {
                EventType::BlockAdded
                | EventType::DeployExpired
                | EventType::DeployProcessed
                | EventType::Fault
                | EventType::FinalitySignature
                | EventType::Step => true,
                EventType::ApiVersion | EventType::DeployAccepted | EventType::Shutdown => false,
            }
        }))
    }
}

async fn partial_connection_test(
    num_of_events_to_send: u64,
    max_subscribers_for_fes: u32,
    allow_partial_connection: bool,
    filter_priority: Option<FilterPriority>,
) -> Option<Vec<EventType>> {
    let test_rng = Box::leak(Box::new(TestRng::new()));

    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");

    // Setup config for the sidecar
    //      - Set the sidecar to reattempt connection only once after a 2 second delay.
    //      - Allow partial based on the value passed to the function.
    let mut testing_config = prepare_config(&temp_storage_dir)
        .configure_retry_settings(1, 2)
        .set_allow_partial_connection(allow_partial_connection);

    // If filter_priority was provided, set it in the config
    if let Some(filter_priority) = filter_priority {
        testing_config.set_filter_priority(filter_priority)
    }

    // Setup config for the Event Stream Server to be used in the Fake Event Stream
    //      - Run it on the port the sidecar is set to connect to.
    //      - The buffer will be the default.
    //      - Limit the max_subscribers to the FakeEventStream as per the value passed to the function.
    let ess_config = EssConfig::new(
        testing_config.connection_port(),
        None,
        Some(max_subscribers_for_fes),
    );

    // Run the Fake Event Stream in another task
    //      - Use the Counted scenario to get the event stream to send the number of events specified in the test.
    tokio::spawn(spin_up_fake_event_stream(
        test_rng,
        ess_config,
        Scenario::Counted(GenericScenarioSettings::new(
            Bound::Counted(num_of_events_to_send),
            None,
        )),
    ));

    // Run the Sidecar in another task with the prepared config.
    tokio::spawn(run(testing_config.inner()));

    // URL for connecting to the Sidecar's event stream.
    let sidecar_bind_address = format!("127.0.0.1:{}", testing_config.event_stream_server_port());

    let test_event_listener = try_connect_listener(sidecar_bind_address).await;

    let mut combined_receiver = test_event_listener.consume_combine_streams().await.unwrap();

    let mut event_types_received = Vec::new();

    while let Some(event) = combined_receiver.recv().await {
        if !matches!(event.data, SseData::ApiVersion(_)) && !matches!(event.data, SseData::Shutdown)
        {
            event_types_received.push(event.data.into())
        }
    }

    if event_types_received.is_empty() {
        None
    } else {
        Some(event_types_received)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
#[ignore]
async fn should_not_attempt_reconnection() {
    let test_rng = Box::leak(Box::new(TestRng::new()));

    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");
    let testing_config = prepare_config(&temp_storage_dir).configure_retry_settings(0, 0);

    let ess_config = EssConfig::new(testing_config.connection_port(), None, None);

    let stream_duration = Duration::from_secs(40);

    tokio::spawn(spin_up_fake_event_stream(
        test_rng,
        ess_config,
        Scenario::Realistic(GenericScenarioSettings::new(
            Bound::Timed(Duration::from_secs(30)),
            Some(Restart::new(
                Duration::from_secs(10),
                Bound::Timed(Duration::from_secs(30)),
            )),
        )),
    ));

    let start_instant = Instant::now();

    run(testing_config.inner())
        .await
        .expect_err("Sidecar should return an Err message on shutdown");

    let time_for_sidecar_to_shutdown = Instant::now() - start_instant;

    println!(
        "Shutdown: {}",
        display_duration(time_for_sidecar_to_shutdown)
    );
    // The sidecar should have shutdown as soon as it receives the Shutdown - making no attempt to reconnect.
    assert!(time_for_sidecar_to_shutdown < stream_duration);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
#[ignore]
async fn should_reconnect_on_time() {
    let test_rng = Box::leak(Box::new(TestRng::new()));

    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");
    // Should provide a 25 second window for the source to restart
    let testing_config = prepare_config(&temp_storage_dir).configure_retry_settings(5, 5);

    let ess_config = EssConfig::new(testing_config.connection_port(), None, None);

    let start_instant = Instant::now();
    let stream_duration = Duration::from_secs(60);

    tokio::spawn(spin_up_fake_event_stream(
        test_rng,
        ess_config,
        Scenario::Realistic(GenericScenarioSettings::new(
            Bound::Timed(Duration::from_secs(30)),
            Some(Restart::new(
                Duration::from_secs(20),
                Bound::Timed(Duration::from_secs(10)),
            )),
        )),
    ));

    run(testing_config.inner())
        .await
        .expect_err("Sidecar should return an Err message on shutdown");

    // The sidecar should have continued to listen after successful reconnection
    assert!(Instant::now() - start_instant > stream_duration);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
#[ignore]
async fn should_shutdown_after_failure_to_reconnect() {
    let test_rng = Box::leak(Box::new(TestRng::new()));

    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");
    // Should provide a 15 second window for the source to restart
    let testing_config = prepare_config(&temp_storage_dir).configure_retry_settings(5, 3);

    let ess_config = EssConfig::new(testing_config.connection_port(), None, None);

    let initial_duration = Duration::from_secs(30);

    tokio::spawn(spin_up_fake_event_stream(
        test_rng,
        ess_config,
        Scenario::Realistic(GenericScenarioSettings::new(
            Bound::Timed(initial_duration),
            Some(Restart::new(Duration::from_secs(20), Bound::Counted(100))),
        )),
    ));

    let start_instant = Instant::now();

    run(testing_config.inner())
        .await
        .expect_err("Sidecar should return an Err message on shutdown");

    let time_for_sidecar_to_shutdown = Instant::now() - start_instant;
    let total_retry_duration = Duration::from_secs(5 * 3);
    let minimum_time = initial_duration + total_retry_duration;
    // The sidecar should have shutdown after completing the configured retries.
    assert!(
        time_for_sidecar_to_shutdown > minimum_time
            && time_for_sidecar_to_shutdown < minimum_time + Duration::from_secs(5)
    );
}

async fn try_connect_to_single_stream(
    url: &str,
) -> EventStream<impl Stream<Item = reqwest::Result<Bytes>> + Sized> {
    let mut event_stream = None;
    for _ in 0..10 {
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

pub(crate) async fn try_connect_listener(bind_address: String) -> EventListener {
    let mut event_listener = None;
    for _ in 0..10 {
        let listener =
            EventListener::new(bind_address.clone(), 3, 3, false, FilterPriority::default()).await;
        if listener.is_ok() {
            event_listener = Some(listener.unwrap());
            break;
        } else {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    event_listener.expect("Unable to connect to stream")
}
