use core::time;
use std::{net::IpAddr, thread, time::Duration};

use bytes::Bytes;
use eventsource_stream::{EventStream, Eventsource};
use futures::Stream;
use futures_util::StreamExt;
use http::StatusCode;
use tempfile::tempdir;
use tokio::{sync::mpsc, time::Instant};

use super::run;
use casper_event_listener::{EventListener, NodeConnectionInterface};
use casper_event_types::sse_data::SseData;
use casper_types::testing::TestRng;
use eventsource_stream::Event;
use std::fmt::Debug;

use crate::{
    event_stream_server::Config as EssConfig,
    testing::{
        fake_event_stream::{
            setup_mock_build_version_server, spin_up_fake_event_stream, status_1_0_0_server,
            status_1_4_10_server, Bound, GenericScenarioSettings, Restart, Scenario,
        },
        shared::EventType,
        simple_sse_server::tests::{
            sse_server_example_data_1_4_10, sse_server_send_n_block_added,
            sse_server_shutdown_1_0_0,
        },
        testing_config::prepare_config,
    },
    types::sse_events::BlockAdded,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn should_not_allow_multiple_connections() {
    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");

    let mut testing_config = prepare_config(&temp_storage_dir);

    testing_config.add_connection(None, None, None);
    testing_config.add_connection(None, None, None);

    let shutdown_error = run(testing_config.inner())
        .await
        .expect_err("Sidecar should return an Err on shutdown");

    assert_eq!(
        shutdown_error.to_string(),
        "Unable to run with multiple connections specified in config"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn should_not_allow_zero_max_attempts() {
    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");

    let mut testing_config = prepare_config(&temp_storage_dir);

    let sse_port_for_node = testing_config.add_connection(None, None, None);

    testing_config.set_retries_for_node(sse_port_for_node, 0, 0);

    let shutdown_error = run(testing_config.inner())
        .await
        .expect_err("Sidecar should return an Err on shutdown");

    assert_eq!(
        shutdown_error.to_string(),
        "Unable to run: max_attempts setting must be above 0 for the sidecar to attempt connection"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn should_bind_to_fake_event_stream_and_shutdown_cleanly() {
    let test_rng = Box::leak(Box::new(TestRng::new()));

    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");
    let mut testing_config = prepare_config(&temp_storage_dir);
    testing_config.add_connection(None, None, None);
    let node_port_for_sse_connection = testing_config.config.connections.get(0).unwrap().sse_port;
    let node_port_for_rest_connection = testing_config.config.connections.get(0).unwrap().rest_port;
    let _ = setup_mock_build_version_server(node_port_for_rest_connection);
    let ess_config = EssConfig::new(node_port_for_sse_connection, None, None);

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
async fn should_allow_client_connection_to_sse() {
    let test_rng = Box::leak(Box::new(TestRng::new()));

    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");
    let mut testing_config = prepare_config(&temp_storage_dir);
    testing_config.add_connection(None, None, None);
    let node_port_for_sse_connection = testing_config.config.connections.get(0).unwrap().sse_port;
    let node_port_for_rest_connection = testing_config.config.connections.get(0).unwrap().rest_port;
    let _ = setup_mock_build_version_server(node_port_for_rest_connection);

    let ess_config = EssConfig::new(node_port_for_sse_connection, None, None);

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
async fn should_send_shutdown_to_sse_client() {
    let test_rng = Box::leak(Box::new(TestRng::new()));

    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");
    let mut testing_config = prepare_config(&temp_storage_dir);
    testing_config.add_connection(None, None, None);
    let node_port_for_sse_connection = testing_config.config.connections.get(0).unwrap().sse_port;
    let node_port_for_rest_connection = testing_config.config.connections.get(0).unwrap().rest_port;
    let _ = setup_mock_build_version_server(node_port_for_rest_connection);

    let ess_config = EssConfig::new(node_port_for_sse_connection, None, None);

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
async fn should_respond_to_rest_query() {
    let test_rng = Box::leak(Box::new(TestRng::new()));

    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");
    let mut testing_config = prepare_config(&temp_storage_dir);
    testing_config.add_connection(None, None, None);
    let node_port_for_sse_connection = testing_config.config.connections.get(0).unwrap().sse_port;
    let node_port_for_rest_connection = testing_config.config.connections.get(0).unwrap().rest_port;
    let _ = setup_mock_build_version_server(node_port_for_rest_connection);

    let ess_config = EssConfig::new(node_port_for_sse_connection, None, None);

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
async fn should_allow_partial_connection_on_one_filter() {
    let received_event_types = partial_connection_test(100, 1, true).await;
    assert!(received_event_types.is_some())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn should_allow_partial_connection_on_two_filters() {
    let received_event_types = partial_connection_test(100, 2, true).await;
    assert!(received_event_types.is_some())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn should_disallow_partial_connection_on_one_filter() {
    let received_event_types = partial_connection_test(100, 1, false).await;
    assert!(received_event_types.is_none())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn should_disallow_partial_connection_on_two_filters() {
    let received_event_types = partial_connection_test(100, 2, false).await;
    assert!(received_event_types.is_none())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn should_not_attempt_reconnection() {
    // Configure the sidecar to make 0 retries
    let max_attempts = 0;
    let delay_between_retries = 0;

    // Configure the Fake Event Stream to shutdown after 30s
    let shutdown_after = Duration::from_secs(30);
    // And then resume after 10s
    let restart_after = Duration::from_secs(10);

    let time_for_sidecar_to_shutdown = reconnection_test(
        max_attempts,
        delay_between_retries,
        Bound::Timed(shutdown_after),
        restart_after,
    )
    .await;

    let total_stream_duration = shutdown_after;

    assert!(time_for_sidecar_to_shutdown < total_stream_duration + Duration::from_secs(3));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn should_successfully_reconnect() {
    // Configure the sidecar to make 5 retries
    let max_attempts = 5;
    let delay_between_retries = 1;

    // And then resume after 5s e.g. less than the total retry window
    let restart_after = Duration::from_secs(5);

    let read_messages =
        reconnection_test_with_port_dropping(max_attempts, delay_between_retries, restart_after)
            .await;
    let length = read_messages.len();
    //The result should only have messages from two rounds of messages
    assert!(length > 61);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn should_fail_to_reconnect() {
    let test_rng = TestRng::new();
    let (node_port_for_sse_connection, node_port_for_rest_connection, event_stream_server_port) =
        start_sidecar_with_retry_config(2, 1).await;
    let (shutdown_tx, test_rng) =
        sse_server_send_n_block_added(node_port_for_sse_connection, 30, 0, test_rng).await;
    let _ = status_1_4_10_server(node_port_for_rest_connection);
    let main_event_stream_url = format!(
        "http://127.0.0.1:{}/events/main?start_from=0",
        event_stream_server_port
    );
    let main_event_stream = try_connect_to_single_stream(&main_event_stream_url).await;
    shutdown_tx.send(()).unwrap();
    let join_handle = tokio::spawn(async move { poll_events(main_event_stream).await });
    thread::sleep(time::Duration::from_secs(8)); //give some time for the old sse server to go away and sidecar to run out of retries
    let (shutdown_tx, _) =
        sse_server_send_n_block_added(node_port_for_sse_connection, 30, 30, test_rng).await;
    thread::sleep(time::Duration::from_secs(3)); //give some time for the data to propagate. Sidecar should be dead by now,
                                                 // but if there's a bug and it isn't we need to give it some time to pick up the second instance of the server
    shutdown_tx.send(()).unwrap();

    let events_received = tokio::join!(join_handle).0.unwrap();
    let length = events_received.len();
    //The result should only have messages from both rounds of messages
    assert_eq!(length, 32);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn should_reconnect() {
    let test_rng = TestRng::new();
    let (node_port_for_sse_connection, node_port_for_rest_connection, event_stream_server_port) =
        start_sidecar_with_retry_config(4, 1).await;
    let (shutdown_tx, test_rng) =
        sse_server_send_n_block_added(node_port_for_sse_connection, 30, 0, test_rng).await;
    let _ = status_1_4_10_server(node_port_for_rest_connection);
    let main_event_stream_url = format!(
        "http://127.0.0.1:{}/events/main?start_from=0",
        event_stream_server_port
    );
    let main_event_stream = try_connect_to_single_stream(&main_event_stream_url).await;
    shutdown_tx.send(()).unwrap();
    let join_handle = tokio::spawn(async move { poll_events(main_event_stream).await });

    thread::sleep(time::Duration::from_secs(5)); //give some time for the old sse server to go away
    let (shutdown_tx, _) =
        sse_server_send_n_block_added(node_port_for_sse_connection, 30, 30, test_rng).await;
    thread::sleep(time::Duration::from_secs(3)); //give some time for the data to propagate
    shutdown_tx.send(()).unwrap();

    let events_received = tokio::join!(join_handle).0.unwrap();
    let length = events_received.len();
    //The result should only have messages from both rounds of messages
    assert!(length > 32);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn if_node_sends_shutdown_sidecar_shutdown_should_be_silenced() {
    let (node_port_for_sse_connection, node_port_for_rest_connection, event_stream_server_port) =
        start_sidecar().await;
    let shutdown_tx = sse_server_shutdown_1_0_0(node_port_for_sse_connection).await;
    let _ = status_1_0_0_server(node_port_for_rest_connection);
    thread::sleep(time::Duration::from_secs(3)); //give some time everything to connect
    let main_event_stream_url =
        format!("http://127.0.0.1:{}/events/main", event_stream_server_port);
    let main_event_stream = try_connect_to_single_stream(&main_event_stream_url).await;
    shutdown_tx.send(()).unwrap();

    let events_received = poll_events(main_event_stream).await;
    assert_eq!(events_received.len(), 2);
    assert!(events_received.get(0).unwrap().contains("\"1.0.0\""));
    assert!(events_received.get(1).unwrap().contains("\"Shutdown\""));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn shutdown_should_be_passed_through() {
    let (node_port_for_sse_connection, node_port_for_rest_connection, event_stream_server_port) =
        start_sidecar().await;
    let shutdown_tx = sse_server_shutdown_1_0_0(node_port_for_sse_connection).await;
    let _ = status_1_0_0_server(node_port_for_rest_connection);
    thread::sleep(time::Duration::from_secs(3)); //give some time everything to connect
    let main_event_stream_url = format!(
        "http://127.0.0.1:{}/events/main?start_from=0",
        event_stream_server_port
    );
    let main_event_stream = try_connect_to_single_stream(&main_event_stream_url).await;
    shutdown_tx.send(()).unwrap();
    thread::sleep(time::Duration::from_secs(1)); //give some time everything to disconnect
    let events_received = poll_events(main_event_stream).await;
    assert_eq!(events_received.len(), 4);
    assert!(events_received.get(0).unwrap().contains("\"1.0.0\""));
    assert!(events_received.get(1).unwrap().contains("\"Shutdown\""));
    assert!(events_received.get(2).unwrap().contains("\"BlockAdded\""));
    assert!(events_received.get(3).unwrap().contains("\"Shutdown\""));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn shutdown_should_be_passed_through_when_versions_change() {
    let (node_port_for_sse_connection, node_port_for_rest_connection, event_stream_server_port) =
        start_sidecar().await;
    let shutdown_tx = sse_server_shutdown_1_0_0(node_port_for_sse_connection).await;
    let change_api_version_tx = status_1_0_0_server(node_port_for_rest_connection);
    thread::sleep(time::Duration::from_secs(3)); //give some time everything to connect
    let main_event_stream_url = format!(
        "http://127.0.0.1:{}/events/main?start_from=0",
        event_stream_server_port
    );
    let main_event_stream = try_connect_to_single_stream(&main_event_stream_url).await;
    shutdown_tx.send(()).unwrap();
    change_api_version_tx
        .send("1.4.10".to_string())
        .await
        .unwrap();
    thread::sleep(time::Duration::from_secs(1)); //give some time everything to disconnect
    let shutdown_tx = sse_server_example_data_1_4_10(node_port_for_sse_connection).await;
    thread::sleep(time::Duration::from_secs(3)); //give some time for sidecar to connect and read data
    shutdown_tx.send(()).unwrap();

    let events_received = poll_events(main_event_stream).await;
    assert_eq!(events_received.len(), 6);
    assert!(events_received.get(0).unwrap().contains("\"1.0.0\""));
    assert!(events_received.get(1).unwrap().contains("\"Shutdown\""));
    assert!(events_received.get(2).unwrap().contains("\"BlockAdded\""));
    assert!(events_received.get(3).unwrap().contains("\"1.4.10\""));
    assert!(events_received.get(4).unwrap().contains("\"BlockAdded\""));
    assert!(events_received.get(5).unwrap().contains("\"Shutdown\""));
}

async fn partial_connection_test(
    num_of_events_to_send: u64,
    max_subscribers_for_fes: u32,
    allow_partial_connection: bool,
) -> Option<Vec<EventType>> {
    let test_rng = Box::leak(Box::new(TestRng::new()));

    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");

    // Setup config for the sidecar
    //      - Set the sidecar to reattempt connection only once after a 2 second delay.
    //      - Allow partial based on the value passed to the function.
    let mut testing_config = prepare_config(&temp_storage_dir);
    testing_config.add_connection(None, None, None);
    let node_port_for_sse_connection = testing_config.config.connections.get(0).unwrap().sse_port;
    let node_port_for_rest_connection = testing_config.config.connections.get(0).unwrap().rest_port;
    let _ = setup_mock_build_version_server(node_port_for_rest_connection);
    testing_config.set_retries_for_node(node_port_for_sse_connection, 1, 2);
    testing_config.set_allow_partial_connection_for_node(
        node_port_for_sse_connection,
        allow_partial_connection,
    );

    // Setup config for the Event Stream Server to be used in the Fake Event Stream
    //      - Run it on the port the sidecar is set to connect to.
    //      - The buffer will be the default.
    //      - Limit the max_subscribers to the FakeEventStream as per the value passed to the function.
    let ess_config = EssConfig::new(
        node_port_for_sse_connection,
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

    let (event_tx, mut event_rx) = mpsc::channel(100);
    let (api_version_tx, _api_version_rx) = mpsc::channel(100);

    let mut test_event_listener = EventListener::new(
        NodeConnectionInterface {
            ip_address: IpAddr::from([127, 0, 0, 1]),
            sse_port: testing_config.event_stream_server_port(),
            rest_port: node_port_for_rest_connection,
        },
        3,
        Duration::from_secs(1),
        false,
        event_tx,
        Duration::from_secs(100),
    );

    tokio::spawn(async move {
        let res = test_event_listener
            .stream_aggregated_events(api_version_tx)
            .await;

        if let Err(error) = res {
            println!("Listener Error: {}", error)
        }
    });

    let mut event_types_received = Vec::new();

    while let Some(event) = event_rx.recv().await {
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

async fn reconnection_test(
    max_attempts: usize,
    delay_between_retries: usize,
    shutdown_after: Bound,
    restart_after: Duration,
) -> Duration {
    let test_rng = Box::leak(Box::new(TestRng::new()));

    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");
    let mut testing_config = prepare_config(&temp_storage_dir);
    testing_config.add_connection(None, None, None);
    let node_port_for_sse_connection = testing_config.config.connections.get(0).unwrap().sse_port;
    let node_port_for_rest_connection = testing_config.config.connections.get(0).unwrap().rest_port;
    let _ = setup_mock_build_version_server(node_port_for_rest_connection);
    testing_config.set_retries_for_node(
        node_port_for_sse_connection,
        max_attempts,
        delay_between_retries,
    );

    let ess_config = EssConfig::new(node_port_for_sse_connection, None, None);

    let fes_handle = tokio::spawn(spin_up_fake_event_stream(
        test_rng,
        ess_config,
        Scenario::Realistic(GenericScenarioSettings::new(
            shutdown_after,
            Some(Restart::new(
                restart_after,
                Bound::Timed(Duration::from_secs(30)),
            )),
        )),
    ));
    let start_instant = Instant::now();
    let sidecar_handle = tokio::spawn(async move {
        let start_instant = Instant::now();
        let maybe_error = run(testing_config.inner()).await;
        maybe_error.expect_err("Sidecar should return an Err message on shutdown");
        Instant::now() - start_instant
    });

    let (_, time_for_sidecar_to_shutdown) = tokio::join!(fes_handle, sidecar_handle);
    let time_it_took_to_restart = Instant::now() - start_instant;
    println!("time_it_took_to_restart {:?}", time_it_took_to_restart);

    time_for_sidecar_to_shutdown.unwrap()
}

async fn reconnection_test_with_port_dropping(
    max_attempts: usize,
    delay_between_retries: usize,
    restart_after: Duration,
) -> Vec<EventType> {
    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");
    let mut testing_config = prepare_config(&temp_storage_dir);
    testing_config.add_connection(None, None, None);
    let node_port_for_sse_connection = testing_config.config.connections.get(0).unwrap().sse_port;
    let node_port_for_rest_connection = testing_config.config.connections.get(0).unwrap().rest_port;
    let _ = setup_mock_build_version_server(node_port_for_rest_connection);
    testing_config.set_retries_for_node(
        node_port_for_sse_connection,
        max_attempts,
        delay_between_retries,
    );
    let event_stream_server_port = testing_config.event_stream_server_port();

    let (event_tx, mut event_rx) = mpsc::channel(1000);
    let (api_version_tx, _api_version_rx) = mpsc::channel(5);
    let mut test_event_listener = EventListener::new(
        NodeConnectionInterface {
            ip_address: IpAddr::from([127, 0, 0, 1]),
            sse_port: event_stream_server_port,
            rest_port: node_port_for_rest_connection,
        },
        3,
        Duration::from_secs(1),
        false,
        event_tx,
        Duration::from_secs(100),
    );

    tokio::spawn(async move {
        let res = test_event_listener
            .stream_aggregated_events(api_version_tx)
            .await;

        if let Err(error) = res {
            println!("Listener Error: {}", error)
        }
    });

    let data_handle = tokio::spawn(async move {
        let mut event_types_received = Vec::new();

        while let Some(event) = event_rx.recv().await {
            event_types_received.push(event.data.into())
        }
        event_types_received
    });

    let ess_config = EssConfig::new(node_port_for_sse_connection, None, None);
    let fes_handle = tokio::spawn(async move {
        let mut test_rng = Box::leak(Box::new(TestRng::new()));
        test_rng = spin_up_fake_event_stream(
            test_rng,
            ess_config.clone(),
            Scenario::Counted(GenericScenarioSettings::new(Bound::Counted(60), None)),
        )
        .await;
        tokio::time::sleep(restart_after).await;
        spin_up_fake_event_stream(
            test_rng,
            ess_config.clone(),
            Scenario::Counted(GenericScenarioSettings::new(Bound::Counted(60), None)),
        )
        .await;
    });

    let sidecar_handle = tokio::spawn(async move {
        let _ = run(testing_config.inner()).await;
    });

    let (_, _, data) = tokio::join!(fes_handle, sidecar_handle, data_handle);
    data.expect("Expecting data")
}

pub async fn try_connect_to_single_stream(
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

pub async fn start_sidecar_with_retry_config(
    max_attempts: usize,
    delay_between_retries: usize,
) -> (u16, u16, u16) {
    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");
    let mut testing_config = prepare_config(&temp_storage_dir);
    testing_config.add_connection(None, None, None);
    let node_port_for_sse_connection = testing_config.config.connections.get(0).unwrap().sse_port;
    let node_port_for_rest_connection = testing_config.config.connections.get(0).unwrap().rest_port;
    testing_config.set_retries_for_node(
        node_port_for_sse_connection,
        max_attempts,
        delay_between_retries,
    );
    testing_config.set_allow_partial_connection_for_node(node_port_for_sse_connection, true);
    tokio::spawn(run(testing_config.inner())); // starting event sidecar
    (
        node_port_for_sse_connection,
        node_port_for_rest_connection,
        testing_config.event_stream_server_port(),
    )
}

pub async fn start_sidecar() -> (u16, u16, u16) {
    start_sidecar_with_retry_config(3, 1).await
}

pub async fn poll_events<E, S>(mut stream: S) -> Vec<String>
where
    E: Debug,
    S: Stream<Item = Result<Event, E>> + Sized + Unpin,
{
    let mut events_received = Vec::new();
    while let Some(Ok(event)) = stream.next().await {
        events_received.push(event.data);
    }
    events_received
}
