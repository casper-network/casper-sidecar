use super::run;
use bytes::Bytes;
use casper_event_listener::{EventListenerBuilder, NodeConnectionInterface};
use casper_event_types::sse_data::{test_support::*, SseData};
use casper_types::testing::TestRng;
use core::time;
use eventsource_stream::{Event, EventStream, Eventsource};
use futures::Stream;
use futures_util::StreamExt;
use http::StatusCode;
use std::{fmt::Debug, net::IpAddr, path::Path, time::Duration};
use tempfile::{tempdir, TempDir};
use tokio::{sync::mpsc, time::Instant};

use crate::{
    event_stream_server::Config as EssConfig,
    sqlite_database::SqliteDatabase,
    testing::{
        fake_event_stream::{
            setup_mock_build_version_server, spin_up_fake_event_stream, Bound,
            GenericScenarioSettings, Restart, Scenario,
        },
        mock_node::tests::MockNode,
        raw_sse_events_utils::tests::{
            random_n_block_added, sse_server_example_1_4_10_data,
            sse_server_example_1_4_10_data_second, sse_server_example_1_4_10_data_third,
            sse_server_example_1_4_9_data_second, sse_server_shutdown_1_0_0_data, EventsWithIds,
        },
        shared::EventType,
        testing_config::{get_port, prepare_config, TestingConfig},
    },
    types::{
        database::DatabaseWriter,
        sse_events::{BlockAdded, Fault},
    },
    utils::{any_string_contains, start_nodes_and_wait, stop_nodes_and_wait, wait_for_n_messages},
};

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
    let (_shutdown_tx, _after_shutdown_rx) =
        setup_mock_build_version_server(node_port_for_rest_connection).await;
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
    let (_shutdown_tx, _after_shutdown_rx) =
        setup_mock_build_version_server(node_port_for_rest_connection).await;

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
async fn should_respond_to_rest_query() {
    let test_rng = Box::leak(Box::new(TestRng::new()));

    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");
    let mut testing_config = prepare_config(&temp_storage_dir);
    testing_config.add_connection(None, None, None);
    let node_port_for_sse_connection = testing_config.config.connections.get(0).unwrap().sse_port;
    let node_port_for_rest_connection = testing_config.config.connections.get(0).unwrap().rest_port;
    let (_shutdown_tx, _after_shutdown_rx) =
        setup_mock_build_version_server(node_port_for_rest_connection).await;

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
    let (
        testing_config,
        _temp_storage_dir,
        node_port_for_sse_connection,
        node_port_for_rest_connection,
        event_stream_server_port,
    ) = build_test_config();

    //MockNode::new should only have /events/main and /events sse endpoints,
    // simulating a situation when a node doesn't expose all endpoints.
    let mut node_mock = MockNode::new(
        "1.4.10".to_string(),
        sse_server_example_1_4_10_data(),
        node_port_for_sse_connection,
        node_port_for_rest_connection,
    )
    .await;
    start_nodes_and_wait(vec![&mut node_mock]).await.unwrap();
    start_sidecar(testing_config).await;
    let (join_handle, receiver) =
        fetch_data_from_endpoint("/events/main?start_from=0", event_stream_server_port).await;
    wait_for_n_messages(1, receiver, Duration::from_secs(30))
        .await
        .unwrap();
    stop_nodes_and_wait(vec![&mut node_mock]).await.unwrap();

    let events_received = tokio::join!(join_handle).0.unwrap();
    assert!(!events_received.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn should_allow_partial_connection_on_two_filters() {
    let received_event_types = partial_connection_test(100, 4, true).await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn should_fail_to_reconnect() {
    let test_rng = TestRng::new();
    let (
        testing_config,
        _temp_storage_dir,
        node_port_for_sse_connection,
        node_port_for_rest_connection,
        event_stream_server_port,
    ) = build_test_config_with_retries(2, 2);
    let (data, test_rng) = random_n_block_added(30, 0, test_rng);
    let mut node_mock = MockNode::new(
        "1.4.10".to_string(),
        data,
        node_port_for_sse_connection,
        node_port_for_rest_connection,
    )
    .await;
    start_nodes_and_wait(vec![&mut node_mock]).await.unwrap();
    start_sidecar(testing_config).await;
    let (join_handle, receiver) =
        fetch_data_from_endpoint("/events/main?start_from=0", event_stream_server_port).await;
    wait_for_n_messages(31, receiver, Duration::from_secs(120))
        .await
        .unwrap();
    stop_nodes_and_wait(vec![&mut node_mock]).await.unwrap();

    tokio::time::sleep(time::Duration::from_secs(15)).await; //give some time for the old sse server to go away

    let (data, _) = random_n_block_added(30, 31, test_rng);
    let mut node_mock = MockNode::new(
        "1.4.10".to_string(),
        data,
        node_port_for_sse_connection,
        node_port_for_rest_connection,
    )
    .await;
    start_nodes_and_wait(vec![&mut node_mock]).await.unwrap();
    tokio::time::sleep(time::Duration::from_secs(3)).await; //give some time for the data to propagate
    stop_nodes_and_wait(vec![&mut node_mock]).await.unwrap();

    let events_received = tokio::join!(join_handle).0.unwrap();
    let length = events_received.len();
    //The result should only have messages from one round of messages
    assert_eq!(length, 32);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn should_reconnect() {
    let test_rng = TestRng::new();
    let (
        testing_config,
        _temp_storage_dir,
        node_port_for_sse_connection,
        node_port_for_rest_connection,
        event_stream_server_port,
    ) = build_test_config_with_retries(10, 1);
    let (data, test_rng) = random_n_block_added(30, 0, test_rng);
    let mut node_mock = MockNode::new(
        "1.4.10".to_string(),
        data,
        node_port_for_sse_connection,
        node_port_for_rest_connection,
    )
    .await;
    start_nodes_and_wait(vec![&mut node_mock]).await.unwrap();
    start_sidecar(testing_config).await;
    let (join_handle, receiver) =
        fetch_data_from_endpoint("/events/main?start_from=0", event_stream_server_port).await;
    let receiver = wait_for_n_messages(31, receiver, Duration::from_secs(120))
        .await
        .unwrap();
    stop_nodes_and_wait(vec![&mut node_mock]).await.unwrap();

    let (data, _) = random_n_block_added(30, 31, test_rng);
    let mut node_mock = MockNode::new(
        "1.4.10".to_string(),
        data,
        node_port_for_sse_connection,
        node_port_for_rest_connection,
    )
    .await;
    start_nodes_and_wait(vec![&mut node_mock]).await.unwrap();
    wait_for_n_messages(31, receiver, Duration::from_secs(120))
        .await
        .unwrap();
    stop_nodes_and_wait(vec![&mut node_mock]).await.unwrap();

    let events_received = tokio::join!(join_handle).0.unwrap();
    let length = events_received.len();
    //The result should only have messages from both rounds of messages
    assert_eq!(length, 63);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn shutdown_should_be_passed_through() {
    let (
        testing_config,
        _temp_storage_dir,
        node_port_for_sse_connection,
        node_port_for_rest_connection,
        event_stream_server_port,
    ) = build_test_config();
    let mut node_mock = MockNode::new(
        "1.0.0".to_string(),
        sse_server_shutdown_1_0_0_data(),
        node_port_for_sse_connection,
        node_port_for_rest_connection,
    )
    .await;
    start_nodes_and_wait(vec![&mut node_mock]).await.unwrap();
    start_sidecar(testing_config).await;
    let (join_handle, receiver) =
        fetch_data_from_endpoint("/events/main?start_from=0", event_stream_server_port).await;
    wait_for_n_messages(2, receiver, Duration::from_secs(120))
        .await
        .unwrap();
    stop_nodes_and_wait(vec![&mut node_mock]).await.unwrap();

    let events_received = tokio::join!(join_handle).0.unwrap();
    assert_eq!(events_received.len(), 3);
    assert!(events_received.get(0).unwrap().contains("\"1.0.0\""));
    assert!(events_received.get(1).unwrap().contains("\"Shutdown\""));
    assert!(events_received.get(2).unwrap().contains("\"BlockAdded\""));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn shutdown_should_be_passed_through_when_versions_change() {
    let (
        testing_config,
        _temp_storage_dir,
        node_port_for_sse_connection,
        node_port_for_rest_connection,
        event_stream_server_port,
    ) = build_test_config();
    let mut node_mock = MockNode::new(
        "1.0.0".to_string(),
        sse_server_shutdown_1_0_0_data(),
        node_port_for_sse_connection,
        node_port_for_rest_connection,
    )
    .await;
    start_nodes_and_wait(vec![&mut node_mock]).await.unwrap();
    start_sidecar(testing_config).await;
    let (join_handle, receiver) =
        fetch_data_from_endpoint("/events/main?start_from=0", event_stream_server_port).await;
    let receiver = wait_for_n_messages(2, receiver, Duration::from_secs(120))
        .await
        .unwrap();
    stop_nodes_and_wait(vec![&mut node_mock]).await.unwrap();
    let mut node_mock = MockNode::new(
        "1.4.10".to_string(),
        sse_server_example_1_4_10_data(),
        node_port_for_sse_connection,
        node_port_for_rest_connection,
    )
    .await;
    start_nodes_and_wait(vec![&mut node_mock]).await.unwrap();
    wait_for_n_messages(1, receiver, Duration::from_secs(120))
        .await
        .unwrap();
    stop_nodes_and_wait(vec![&mut node_mock]).await.unwrap();

    let events_received = tokio::join!(join_handle).0.unwrap();
    assert_eq!(events_received.len(), 5);
    assert!(events_received.get(0).unwrap().contains("\"1.0.0\""));
    assert!(events_received.get(1).unwrap().contains("\"Shutdown\""));
    assert!(events_received.get(2).unwrap().contains("\"BlockAdded\""));
    assert!(events_received.get(3).unwrap().contains("\"1.4.10\""));
    assert!(events_received.get(4).unwrap().contains("\"BlockAdded\""));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn should_produce_shutdown_to_sidecar_endpoint() {
    let (
        testing_config,
        _temp_storage_dir,
        node_port_for_sse_connection,
        node_port_for_rest_connection,
        event_stream_server_port,
    ) = build_test_config();
    let mut node_mock = MockNode::new(
        "1.4.10".to_string(),
        sse_server_example_1_4_10_data(),
        node_port_for_sse_connection,
        node_port_for_rest_connection,
    )
    .await;
    start_nodes_and_wait(vec![&mut node_mock]).await.unwrap();
    start_sidecar(testing_config).await;
    let (join_handle, receiver) =
        fetch_data_from_endpoint("/events/sidecar", event_stream_server_port).await;
    stop_nodes_and_wait(vec![&mut node_mock]).await.unwrap();
    wait_for_n_messages(2, receiver, Duration::from_secs(120))
        .await
        .unwrap();

    let events_received = tokio::join!(join_handle).0.unwrap();
    assert_eq!(events_received.len(), 2);
    assert!(events_received
        .get(0)
        .unwrap()
        .contains("\"SidecarVersion\""));
    assert!(events_received.get(1).unwrap().contains("\"Shutdown\""));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sidecar_should_use_start_from_if_database_is_empty() {
    let (
        testing_config,
        _temp_storage_dir,
        node_port_for_sse_connection,
        node_port_for_rest_connection,
        event_stream_server_port,
    ) = build_test_config();
    let data = vec![(
        Some("2".to_string()),
        example_block_added_1_4_10(BLOCK_HASH_3, "3"),
    )];
    let mut node_mock = MockNode::new_with_cache(
        "1.4.10".to_string(),
        data,
        sse_server_example_1_4_10_data(),
        node_port_for_sse_connection,
        node_port_for_rest_connection,
    )
    .await;
    start_nodes_and_wait(vec![&mut node_mock]).await.unwrap();
    start_sidecar(testing_config).await;
    let (join_handle, receiver) =
        fetch_data_from_endpoint("/events/main?start_from=0", event_stream_server_port).await;
    wait_for_n_messages(2, receiver, Duration::from_secs(120))
        .await
        .unwrap();
    stop_nodes_and_wait(vec![&mut node_mock]).await.unwrap();
    let events_received = tokio::join!(join_handle).0.unwrap();
    assert_eq!(events_received.len(), 3);
    assert!(events_received.get(0).unwrap().contains("\"1.4.10\""));
    assert!(events_received.get(1).unwrap().contains("\"BlockAdded\""));
    assert!(events_received.get(2).unwrap().contains("\"BlockAdded\""));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sidecar_should_not_use_start_from_if_database_is_not_empty() {
    let mut rng = TestRng::new();
    let (
        testing_config,
        _temp_storage_dir,
        node_port_for_sse_connection,
        node_port_for_rest_connection,
        event_stream_server_port,
    ) = build_test_config();
    //Prepopulating database
    let sqlite_database = SqliteDatabase::new(
        Path::new(&testing_config.config.storage.storage_path),
        testing_config.config.storage.sqlite_config.clone(),
    )
    .await
    .expect("database should start");
    sqlite_database
        .save_fault(Fault::random(&mut rng), 0, "127.0.0.1".to_string())
        .await
        .unwrap();
    let mut node_mock = MockNode::new_with_cache(
        "1.4.10".to_string(),
        sse_server_example_1_4_10_data_second(),
        sse_server_example_1_4_10_data(),
        node_port_for_sse_connection,
        node_port_for_rest_connection,
    )
    .await;
    start_nodes_and_wait(vec![&mut node_mock]).await.unwrap();
    start_sidecar(testing_config).await;
    let (join_handle, receiver) =
        fetch_data_from_endpoint("/events/main?start_from=0", event_stream_server_port).await;
    wait_for_n_messages(1, receiver, Duration::from_secs(120))
        .await
        .unwrap();
    stop_nodes_and_wait(vec![&mut node_mock]).await.unwrap();

    let events_received = tokio::join!(join_handle).0.unwrap();
    assert_eq!(events_received.len(), 2);
    //Should not have data from node cache
    assert!(events_received.get(0).unwrap().contains("\"1.4.10\""));
    assert!(events_received.get(1).unwrap().contains("\"BlockAdded\""));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn sidecar_should_connect_to_multiple_nodes() {
    let (sse_port_1, rest_port_1, mut mock_node_1) =
        build_1_4_10(sse_server_example_1_4_10_data()).await;
    mock_node_1.start().await;
    let (sse_port_2, rest_port_2, mut mock_node_2) =
        build_1_4_10(sse_server_example_1_4_10_data_second()).await;
    mock_node_2.start().await;
    let (sse_port_3, rest_port_3, mut mock_node_3) =
        build_1_4_10(sse_server_example_1_4_10_data_third()).await;
    mock_node_3.start().await;
    let (testing_config, event_stream_server_port, _temp_storage_dir) =
        build_testing_config_based_on_ports(vec![
            (sse_port_1, rest_port_1),
            (sse_port_2, rest_port_2),
            (sse_port_3, rest_port_3),
        ]);
    start_sidecar(testing_config).await;
    let (join_handle, receiver) =
        fetch_data_from_endpoint("/events/main?start_from=0", event_stream_server_port).await;
    wait_for_n_messages(3, receiver, Duration::from_secs(120))
        .await
        .unwrap();
    stop_nodes_and_wait(vec![&mut mock_node_1, &mut mock_node_2, &mut mock_node_3])
        .await
        .unwrap();

    let events_received = tokio::join!(join_handle).0.unwrap();
    let length = events_received.len();
    assert_eq!(length, 4);
    assert!(events_received.get(0).unwrap().contains("\"1.4.10\""));
    assert!(any_string_contains(
        &events_received,
        format!("\"{BLOCK_HASH_2}\"")
    ));
    assert!(any_string_contains(
        &events_received,
        format!("\"{BLOCK_HASH_3}\"")
    ));
    assert!(any_string_contains(
        &events_received,
        format!("\"{BLOCK_HASH_4}\"")
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn sidecar_should_not_downgrade_api_version_when_new_nodes_disconnect() {
    let (sse_port_1, rest_port_1, mut mock_node_1) =
        build_1_4_10(sse_server_example_1_4_10_data()).await;
    mock_node_1.start().await;
    let (sse_port_2, rest_port_2, mut mock_node_2) =
        build_1_4_10(sse_server_example_1_4_9_data_second()).await;
    let (testing_config, event_stream_server_port, _temp_storage_dir) =
        build_testing_config_based_on_ports(vec![
            (sse_port_1, rest_port_1),
            (sse_port_2, rest_port_2),
        ]);
    start_sidecar(testing_config).await;
    let (join_handle, receiver) =
        fetch_data_from_endpoint("/events/main?start_from=0", event_stream_server_port).await;
    let receiver = wait_for_n_messages(1, receiver, Duration::from_secs(120))
        .await
        .unwrap();
    mock_node_1.stop().await;
    mock_node_2.start().await;
    wait_for_n_messages(1, receiver, Duration::from_secs(120))
        .await
        .unwrap();
    mock_node_2.stop().await;
    let events_received = tokio::join!(join_handle).0.unwrap();
    let length = events_received.len();
    assert_eq!(length, 3);
    assert!(events_received.get(0).unwrap().contains("\"1.4.10\""));
    assert!(any_string_contains(
        &events_received,
        format!("\"{BLOCK_HASH_2}\"")
    ));
    assert!(any_string_contains(
        &events_received,
        format!("\"{BLOCK_HASH_3}\"")
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sidecar_should_report_only_one_api_version_if_there_was_no_update() {
    let (sse_port_1, rest_port_1, mut mock_node_1) =
        build_1_4_10(sse_server_example_1_4_10_data()).await;
    let (sse_port_2, rest_port_2, mut mock_node_2) =
        build_1_4_10(sse_server_example_1_4_10_data_second()).await;
    start_nodes_and_wait(vec![&mut mock_node_1, &mut mock_node_2])
        .await
        .unwrap();
    let (testing_config, event_stream_server_port, _temp_storage_dir) =
        build_testing_config_based_on_ports(vec![
            (sse_port_1, rest_port_1),
            (sse_port_2, rest_port_2),
        ]);
    start_sidecar(testing_config).await;
    let (join_handle, receiver) =
        fetch_data_from_endpoint("/events/main?start_from=0", event_stream_server_port).await;
    wait_for_n_messages(2, receiver, Duration::from_secs(120))
        .await
        .unwrap();
    stop_nodes_and_wait(vec![&mut mock_node_1, &mut mock_node_2])
        .await
        .unwrap();
    let events_received = tokio::join!(join_handle).0.unwrap();
    let length = events_received.len();
    assert_eq!(length, 3);
    assert!(events_received.get(0).unwrap().contains("\"1.4.10\""));
    assert!(any_string_contains(
        &events_received,
        format!("\"{BLOCK_HASH_2}\"")
    ));
    assert!(any_string_contains(
        &events_received,
        format!("\"{BLOCK_HASH_3}\"")
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn sidecar_should_connect_to_multiple_nodes_even_if_some_of_them_dont_respond() {
    let (sse_port_1, rest_port_1, mut mock_node_1) =
        build_1_4_10(sse_server_example_1_4_10_data()).await;
    let (sse_port_2, rest_port_2, mut mock_node_2) =
        build_1_4_10(sse_server_example_1_4_10_data_second()).await;
    start_nodes_and_wait(vec![&mut mock_node_1, &mut mock_node_2])
        .await
        .unwrap();
    let (testing_config, event_stream_server_port, _temp_storage_dir) =
        build_testing_config_based_on_ports(vec![
            (sse_port_1, rest_port_1),
            (sse_port_2, rest_port_2),
            (8888, 9999), //Ports which should be not occupied
        ]);
    start_sidecar(testing_config).await;
    let (join_handle, receiver) =
        fetch_data_from_endpoint("/events/main?start_from=0", event_stream_server_port).await;
    wait_for_n_messages(2, receiver, Duration::from_secs(120))
        .await
        .unwrap();
    stop_nodes_and_wait(vec![&mut mock_node_1, &mut mock_node_2])
        .await
        .unwrap();

    let events_received = tokio::join!(join_handle).0.unwrap();
    let length = events_received.len();
    assert_eq!(length, 3);
    assert!(events_received.get(0).unwrap().contains("\"1.4.10\""));
    assert!(any_string_contains(
        &events_received,
        format!("\"{BLOCK_HASH_2}\"")
    ));
    assert!(any_string_contains(
        &events_received,
        format!("\"{BLOCK_HASH_3}\"")
    ));
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
    let (_shutdown_tx, _after_shutdown_rx) =
        setup_mock_build_version_server(node_port_for_rest_connection).await;
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
    let mut test_event_listener = EventListenerBuilder {
        node: NodeConnectionInterface {
            ip_address: IpAddr::from([127, 0, 0, 1]),
            sse_port: testing_config.event_stream_server_port(),
            rest_port: node_port_for_rest_connection,
        },
        max_connection_attempts: 3,
        delay_between_attempts: Duration::from_secs(1),
        allow_partial_connection: false,
        sse_event_sender: event_tx,
        connection_timeout: Duration::from_secs(100),
        sleep_between_keep_alive_checks: Duration::from_secs(100),
        no_message_timeout: Duration::from_secs(100),
    }
    .build();

    tokio::spawn(async move {
        let res = test_event_listener.stream_aggregated_events(false).await;

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
    let (_shutdown_tx, _after_shutdown_rx) =
        setup_mock_build_version_server(node_port_for_rest_connection).await;
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
    let sidecar_handle = tokio::spawn(async move {
        let start_instant = Instant::now();
        let maybe_error = run(testing_config.inner()).await;
        maybe_error.expect_err("Sidecar should return an Err message on shutdown");
        Instant::now() - start_instant
    });

    let (_, time_for_sidecar_to_shutdown) = tokio::join!(fes_handle, sidecar_handle);
    time_for_sidecar_to_shutdown.unwrap()
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
pub fn build_test_config() -> (TestingConfig, TempDir, u16, u16, u16) {
    build_test_config_with_retries(10, 1)
}

pub fn build_test_config_without_connections() -> (TestingConfig, TempDir, u16) {
    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");
    let testing_config = prepare_config(&temp_storage_dir);
    let event_stream_server_port = testing_config.event_stream_server_port();
    (testing_config, temp_storage_dir, event_stream_server_port)
}

pub fn build_test_config_with_retries(
    max_attempts: usize,
    delay_between_retries: usize,
) -> (TestingConfig, TempDir, u16, u16, u16) {
    let (mut testing_config, temp_storage_dir, event_stream_server_port) =
        build_test_config_without_connections();
    testing_config.add_connection(None, None, None);
    let node_port_for_sse_connection = testing_config.config.connections.get(0).unwrap().sse_port;
    let node_port_for_rest_connection = testing_config.config.connections.get(0).unwrap().rest_port;
    testing_config.set_retries_for_node(
        node_port_for_sse_connection,
        max_attempts,
        delay_between_retries,
    );
    testing_config.set_allow_partial_connection_for_node(node_port_for_sse_connection, true);
    (
        testing_config,
        temp_storage_dir,
        node_port_for_sse_connection,
        node_port_for_rest_connection,
        event_stream_server_port,
    )
}

pub async fn start_sidecar(testing_config: TestingConfig) {
    tokio::spawn(async move {
        let _r = run(testing_config.inner()).await;
    }); // starting event sidecar
}

pub async fn poll_events<E, S>(mut stream: S, sender: mpsc::Sender<()>) -> Vec<String>
where
    E: Debug,
    S: Stream<Item = Result<Event, E>> + Sized + Unpin,
{
    let mut events_received = Vec::new();
    while let Some(Ok(event)) = stream.next().await {
        if !event.data.contains("ApiVersion") {
            let _ = sender.send(()).await;
        }
        events_received.push(event.data);
    }
    events_received
}

pub async fn fetch_data_from_endpoint(
    endpoint: &str,
    port: u16,
) -> (tokio::task::JoinHandle<Vec<String>>, mpsc::Receiver<()>) {
    let local_endpoint = endpoint.to_owned();
    let (sender, receiver) = mpsc::channel(100);
    let join = tokio::spawn(async move {
        let main_event_stream_url = format!("http://127.0.0.1:{}{}", port, local_endpoint);
        let main_event_stream = try_connect_to_single_stream(&main_event_stream_url).await;
        poll_events(main_event_stream, sender).await
    });
    (join, receiver)
}

pub async fn build_1_4_10(data_of_node: EventsWithIds) -> (u16, u16, MockNode) {
    let node_port_for_sse_connection = get_port();
    let node_port_for_rest_connection = get_port();
    let node_mock = MockNode::new(
        "1.4.10".to_string(),
        data_of_node,
        node_port_for_sse_connection,
        node_port_for_rest_connection,
    )
    .await;
    (
        node_port_for_sse_connection,
        node_port_for_rest_connection,
        node_mock,
    )
}

pub fn build_testing_config_based_on_ports(
    ports_of_nodes: Vec<(u16, u16)>,
) -> (TestingConfig, u16, TempDir) {
    let (mut testing_config, temp_storage_dir, event_stream_server_port) =
        build_test_config_without_connections();
    for (sse_port, rest_port) in ports_of_nodes {
        testing_config.add_connection(None, Some(sse_port), Some(rest_port));
        testing_config.set_retries_for_node(sse_port, 5, 2);
        testing_config.set_allow_partial_connection_for_node(sse_port, true);
    }
    (testing_config, event_stream_server_port, temp_storage_dir)
}
