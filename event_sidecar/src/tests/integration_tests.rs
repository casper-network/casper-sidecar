use bytes::Bytes;
use casper_event_types::sse_data::test_support::*;
use casper_types::testing::TestRng;
use core::time;
use eventsource_stream::{Event, EventStream, Eventsource};
use futures::Stream;
use futures_util::StreamExt;
use http::StatusCode;
use std::{fmt::Debug, time::Duration};
use tempfile::{tempdir, TempDir};
use tokio::{sync::mpsc, time::sleep};

use crate::{
    database::{sqlite_database::SqliteDatabase, types::SseEnvelope},
    run,
    testing::{
        mock_node::tests::{MockNode, MockNodeBuilder},
        raw_sse_events_utils::tests::{
            random_n_block_added, sse_server_example_2_0_0_data,
            sse_server_example_2_0_0_data_second, sse_server_example_2_0_0_data_third,
            sse_server_shutdown_2_0_0_data, EventsWithIds,
        },
        testing_config::{prepare_config, TestingConfig},
    },
    types::{
        database::{Database, DatabaseWriter},
        sse_events::{BlockAdded, Fault},
    },
    utils::tests::{
        any_string_contains, build_test_config, build_test_config_with_retries,
        build_test_config_without_connections, start_nodes_and_wait, start_sidecar,
        start_sidecar_with_rest_api, stop_nodes_and_wait, wait_for_n_messages,
    },
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn should_not_allow_zero_max_attempts() {
    let temp_storage_dir = tempdir().expect("Should have created a temporary storage directory");

    let mut testing_config = prepare_config(&temp_storage_dir);

    let sse_port_for_node = testing_config.add_connection(None, None, None);

    testing_config.set_retries_for_node(sse_port_for_node, 0, 0);
    let sqlite_database = SqliteDatabase::new_from_config(&testing_config.storage_config)
        .await
        .expect("database should start");
    let shutdown_error = run(
        testing_config.inner(),
        Database::SqliteDatabaseWrapper(sqlite_database),
        testing_config.storage_config.get_storage_path().clone(),
    )
    .await
    .expect_err("Sidecar should return an Err on shutdown");

    assert_eq!(
        shutdown_error.to_string(),
        "Unable to run: max_attempts setting must be above 0 for the sidecar to attempt connection"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_sidecar_when_only_node_shuts_down_then_shut_down() {
    let (
        testing_config,
        _temp_storage_dir,
        node_port_for_sse_connection,
        node_port_for_rest_connection,
        event_stream_server_port,
    ) = build_test_config();

    //MockNode::new should only have /events and /events sse endpoints,
    // simulating a situation when a node doesn't expose all endpoints.
    let mut node_mock = MockNodeBuilder::build_example_2_0_0_node(
        node_port_for_sse_connection,
        node_port_for_rest_connection,
    );
    start_nodes_and_wait(vec![&mut node_mock]).await;
    let sidecar_join = start_sidecar(testing_config).await;
    let (_, receiver) =
        fetch_data_from_endpoint("/events?start_from=0", event_stream_server_port).await;
    wait_for_n_messages(1, receiver, Duration::from_secs(30)).await;
    stop_nodes_and_wait(vec![&mut node_mock]).await;

    let shutdown_err = sidecar_join
        .await
        .unwrap()
        .expect_err("Sidecar should return an Err message on shutdown");

    assert_eq!(
        shutdown_err.to_string(),
        "Connected node(s) are unavailable"
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn should_allow_client_connection_to_sse() {
    let (
        testing_config,
        _temp_storage_dir,
        node_port_for_sse_connection,
        node_port_for_rest_connection,
        event_stream_server_port,
    ) = build_test_config();
    let mut node_mock = MockNodeBuilder::build_example_2_0_0_node(
        node_port_for_sse_connection,
        node_port_for_rest_connection,
    );
    start_nodes_and_wait(vec![&mut node_mock]).await;
    start_sidecar(testing_config).await;
    let (join_handle, receiver) =
        fetch_data_from_endpoint("/events?start_from=0", event_stream_server_port).await;
    wait_for_n_messages(1, receiver, Duration::from_secs(30)).await;
    stop_nodes_and_wait(vec![&mut node_mock]).await;

    let events_received = tokio::join!(join_handle).0.unwrap();
    assert_eq!(events_received.len(), 2);
    assert!(
        events_received[0].contains("\"ApiVersion\""),
        "First event should be ApiVersion"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn should_respond_to_rest_query() {
    let (
        testing_config,
        _temp_storage_dir,
        node_port_for_sse_connection,
        node_port_for_rest_connection,
        event_stream_server_port,
    ) = build_test_config();
    let sidecar_rest_server_port = testing_config.rest_api_server_config.port;
    let mut node_mock = MockNodeBuilder::build_example_2_0_0_node(
        node_port_for_sse_connection,
        node_port_for_rest_connection,
    );
    start_nodes_and_wait(vec![&mut node_mock]).await;
    start_sidecar_with_rest_api(testing_config).await;
    let (_, receiver) =
        fetch_data_from_endpoint("/events?start_from=0", event_stream_server_port).await;
    wait_for_n_messages(1, receiver, Duration::from_secs(30)).await;

    let block_request_url = format!("http://127.0.0.1:{}/block", sidecar_rest_server_port);
    let response = reqwest::Client::new()
        .get(&block_request_url)
        .send()
        .await
        .expect("Error requesting the /block endpoint");

    assert!(response.status() == StatusCode::OK);

    let response_bytes = response
        .bytes()
        .await
        .expect("Should have got bytes from response");
    serde_json::from_slice::<SseEnvelope<BlockAdded>>(&response_bytes)
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
    let mut node_mock = MockNodeBuilder::build_example_2_0_0_node(
        node_port_for_sse_connection,
        node_port_for_rest_connection,
    );
    start_nodes_and_wait(vec![&mut node_mock]).await;
    start_sidecar(testing_config).await;
    let (join_handle, receiver) =
        fetch_data_from_endpoint("/events?start_from=0", event_stream_server_port).await;
    wait_for_n_messages(1, receiver, Duration::from_secs(30)).await;
    stop_nodes_and_wait(vec![&mut node_mock]).await;

    let events_received = tokio::join!(join_handle).0.unwrap();
    assert!(!events_received.is_empty());
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
    let (data_of_node, test_rng) = random_n_block_added(30, 0, test_rng);
    let mut node_mock = MockNodeBuilder {
        version: "2.0.0".to_string(),
        network_name: "network-1".to_string(),
        data_of_node,
        cache_of_node: None,
        sse_port: Some(node_port_for_sse_connection),
        rest_port: Some(node_port_for_rest_connection),
    }
    .build();
    start_nodes_and_wait(vec![&mut node_mock]).await;
    start_sidecar(testing_config).await;
    let (join_handle, receiver) =
        fetch_data_from_endpoint("/events?start_from=0", event_stream_server_port).await;
    wait_for_n_messages(31, receiver, Duration::from_secs(120)).await;
    stop_nodes_and_wait(vec![&mut node_mock]).await;

    tokio::time::sleep(time::Duration::from_secs(15)).await; //give some time for the old sse server to go away

    let (data_of_node, _) = random_n_block_added(30, 31, test_rng);
    let mut node_mock = MockNodeBuilder {
        version: "2.0.0".to_string(),
        network_name: "network-1".to_string(),
        data_of_node,
        cache_of_node: None,
        sse_port: Some(node_port_for_sse_connection),
        rest_port: Some(node_port_for_rest_connection),
    }
    .build();
    start_nodes_and_wait(vec![&mut node_mock]).await;
    tokio::time::sleep(time::Duration::from_secs(3)).await; //give some time for the data to propagate
    stop_nodes_and_wait(vec![&mut node_mock]).await;

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
    let (data_of_node, test_rng) = random_n_block_added(30, 0, test_rng);
    let mut node_mock = MockNodeBuilder {
        version: "2.0.0".to_string(),
        network_name: "network-1".to_string(),
        data_of_node,
        cache_of_node: None,
        sse_port: Some(node_port_for_sse_connection),
        rest_port: Some(node_port_for_rest_connection),
    }
    .build();
    start_nodes_and_wait(vec![&mut node_mock]).await;
    start_sidecar(testing_config).await;
    let (join_handle, receiver) =
        fetch_data_from_endpoint("/events?start_from=0", event_stream_server_port).await;
    let receiver = wait_for_n_messages(31, receiver, Duration::from_secs(120)).await;
    stop_nodes_and_wait(vec![&mut node_mock]).await;

    let (data_of_node, _) = random_n_block_added(30, 31, test_rng);
    let mut node_mock = MockNodeBuilder {
        version: "2.0.0".to_string(),
        network_name: "network-1".to_string(),
        data_of_node,
        cache_of_node: None,
        sse_port: Some(node_port_for_sse_connection),
        rest_port: Some(node_port_for_rest_connection),
    }
    .build();
    start_nodes_and_wait(vec![&mut node_mock]).await;
    wait_for_n_messages(31, receiver, Duration::from_secs(120)).await;
    stop_nodes_and_wait(vec![&mut node_mock]).await;

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
    let mut node_mock = MockNodeBuilder {
        version: "2.0.0".to_string(),
        network_name: "network-1".to_string(),
        data_of_node: sse_server_shutdown_2_0_0_data(),
        cache_of_node: None,
        sse_port: Some(node_port_for_sse_connection),
        rest_port: Some(node_port_for_rest_connection),
    }
    .build();
    start_nodes_and_wait(vec![&mut node_mock]).await;
    start_sidecar(testing_config).await;
    let (join_handle, receiver) =
        fetch_data_from_endpoint("/events?start_from=0", event_stream_server_port).await;
    wait_for_n_messages(2, receiver, Duration::from_secs(120)).await;
    stop_nodes_and_wait(vec![&mut node_mock]).await;

    let events_received = tokio::join!(join_handle).0.unwrap();
    assert_eq!(events_received.len(), 3);
    assert!(events_received.first().unwrap().contains("\"2.0.0\""));
    assert!(events_received.get(1).unwrap().contains("\"Shutdown\""));
    assert!(events_received.get(2).unwrap().contains("\"BlockAdded\""));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn connecting_to_node_prior_to_2_0_0_should_fail() {
    let (
        testing_config,
        _temp_storage_dir,
        node_port_for_sse_connection,
        node_port_for_rest_connection,
        event_stream_server_port,
    ) = build_test_config();
    let mut node_mock = MockNodeBuilder {
        version: "1.9.9".to_string(),
        network_name: "network-1".to_string(),
        data_of_node: sse_server_shutdown_2_0_0_data(),
        cache_of_node: None,
        sse_port: Some(node_port_for_sse_connection),
        rest_port: Some(node_port_for_rest_connection),
    }
    .build();
    start_nodes_and_wait(vec![&mut node_mock]).await;
    start_sidecar(testing_config).await;
    let (join_handle, _) = fetch_data_from_endpoint_with_panic_flag(
        "/events?start_from=0",
        event_stream_server_port,
        false,
    )
    .await;
    sleep(Duration::from_secs(10)).await; //Give some time for sidecar to read data from node (which it actually shouldn't do in this scenario)
    stop_nodes_and_wait(vec![&mut node_mock]).await;

    let events_received = tokio::join!(join_handle).0.unwrap();
    assert_eq!(events_received.len(), 0);
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
    let mut node_mock = MockNodeBuilder {
        version: "2.0.0".to_string(),
        network_name: "network-1".to_string(),
        data_of_node: sse_server_shutdown_2_0_0_data(),
        cache_of_node: None,
        sse_port: Some(node_port_for_sse_connection),
        rest_port: Some(node_port_for_rest_connection),
    }
    .build();
    start_nodes_and_wait(vec![&mut node_mock]).await;
    start_sidecar(testing_config).await;
    let (join_handle, receiver) =
        fetch_data_from_endpoint("/events?start_from=0", event_stream_server_port).await;
    let receiver = wait_for_n_messages(3, receiver, Duration::from_secs(120)).await;
    stop_nodes_and_wait(vec![&mut node_mock]).await;
    let mut node_mock = MockNodeBuilder::build_example_node_with_version(
        Some(node_port_for_sse_connection),
        Some(node_port_for_rest_connection),
        "2.0.1",
        "network-1",
    );
    start_nodes_and_wait(vec![&mut node_mock]).await;
    wait_for_n_messages(2, receiver, Duration::from_secs(120)).await;
    stop_nodes_and_wait(vec![&mut node_mock]).await;

    let events_received = tokio::join!(join_handle).0.unwrap();
    assert_eq!(events_received.len(), 5);
    assert!(events_received.first().unwrap().contains("\"2.0.0\""));
    assert!(events_received.get(1).unwrap().contains("\"Shutdown\""));
    assert!(events_received.get(2).unwrap().contains("\"BlockAdded\""));
    assert!(events_received.get(3).unwrap().contains("\"2.0.1\""));
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
    let mut node_mock = MockNodeBuilder::build_example_2_0_0_node(
        node_port_for_sse_connection,
        node_port_for_rest_connection,
    );
    start_nodes_and_wait(vec![&mut node_mock]).await;
    start_sidecar(testing_config).await;
    let (join_handle, receiver) =
        fetch_data_from_endpoint("/events/sidecar", event_stream_server_port).await;
    stop_nodes_and_wait(vec![&mut node_mock]).await;
    wait_for_n_messages(2, receiver, Duration::from_secs(120)).await;

    let events_received = tokio::join!(join_handle).0.unwrap();
    assert_eq!(events_received.len(), 2);
    assert!(events_received
        .first()
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
    let data_of_node = vec![(
        Some("2".to_string()),
        example_block_added_2_0_0(BLOCK_HASH_3, "3"),
    )];
    let mut node_mock = MockNodeBuilder {
        version: "2.0.0".to_string(),
        network_name: "network-1".to_string(),
        data_of_node,
        cache_of_node: Some(sse_server_example_2_0_0_data()),
        sse_port: Some(node_port_for_sse_connection),
        rest_port: Some(node_port_for_rest_connection),
    }
    .build();
    start_nodes_and_wait(vec![&mut node_mock]).await;
    start_sidecar(testing_config).await;
    let (join_handle, receiver) =
        fetch_data_from_endpoint("/events?start_from=0", event_stream_server_port).await;
    wait_for_n_messages(2, receiver, Duration::from_secs(120)).await;
    stop_nodes_and_wait(vec![&mut node_mock]).await;
    let events_received = tokio::join!(join_handle).0.unwrap();
    assert_eq!(events_received.len(), 3);
    assert!(events_received.first().unwrap().contains("\"2.0.0\""));
    assert!(events_received.get(1).unwrap().contains("\"BlockAdded\""));
    assert!(events_received.get(2).unwrap().contains("\"BlockAdded\""));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sidecar_should_use_start_from_if_database_is_not_empty() {
    let mut rng = TestRng::new();
    let (
        testing_config,
        _temp_storage_dir,
        node_port_for_sse_connection,
        node_port_for_rest_connection,
        event_stream_server_port,
    ) = build_test_config();
    //Prepopulating database
    let sqlite_database = SqliteDatabase::new_from_config(&testing_config.storage_config)
        .await
        .expect("database should start");
    sqlite_database
        .save_fault(
            Fault::random(&mut rng),
            0,
            "127.0.0.1".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string(),
        )
        .await
        .unwrap();
    let mut node_mock = MockNodeBuilder {
        version: "2.0.0".to_string(),
        network_name: "network-1".to_string(),
        data_of_node: sse_server_example_2_0_0_data_second(),
        cache_of_node: Some(sse_server_example_2_0_0_data()),
        sse_port: Some(node_port_for_sse_connection),
        rest_port: Some(node_port_for_rest_connection),
    }
    .build();
    start_nodes_and_wait(vec![&mut node_mock]).await;
    start_sidecar(testing_config).await;
    let (join_handle, receiver) =
        fetch_data_from_endpoint("/events?start_from=0", event_stream_server_port).await;
    wait_for_n_messages(1, receiver, Duration::from_secs(120)).await;
    stop_nodes_and_wait(vec![&mut node_mock]).await;

    let events_received = tokio::join!(join_handle).0.unwrap();
    assert_eq!(events_received.len(), 3);
    assert!(events_received.first().unwrap().contains("\"2.0.0\""));
    assert!(events_received.get(1).unwrap().contains("\"BlockAdded\""));
    assert!(events_received.get(2).unwrap().contains("\"BlockAdded\""));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn sidecar_should_connect_to_multiple_nodes() {
    let (sse_port_1, rest_port_1, mut mock_node_1) =
        build_2_0_0(sse_server_example_2_0_0_data()).await;
    mock_node_1.start().await;
    let (sse_port_2, rest_port_2, mut mock_node_2) =
        build_2_0_0(sse_server_example_2_0_0_data_second()).await;
    mock_node_2.start().await;
    let (sse_port_3, rest_port_3, mut mock_node_3) =
        build_2_0_0(sse_server_example_2_0_0_data_third()).await;
    mock_node_3.start().await;
    let (testing_config, event_stream_server_port, _temp_storage_dir) =
        build_testing_config_based_on_ports(vec![
            (sse_port_1, rest_port_1),
            (sse_port_2, rest_port_2),
            (sse_port_3, rest_port_3),
        ]);
    start_sidecar(testing_config).await;
    let (join_handle, receiver) =
        fetch_data_from_endpoint("/events?start_from=0", event_stream_server_port).await;
    wait_for_n_messages(4, receiver, Duration::from_secs(120)).await;
    stop_nodes_and_wait(vec![&mut mock_node_1, &mut mock_node_2, &mut mock_node_3]).await;

    let events_received = tokio::join!(join_handle).0.unwrap();
    let length = events_received.len();
    assert_eq!(length, 4);
    assert!(events_received.first().unwrap().contains("\"2.0.0\""));
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
        build_2_0_0(sse_server_example_2_0_0_data()).await;
    mock_node_1.start().await;
    let (sse_port_2, rest_port_2, mut mock_node_2) =
        build_2_0_0(sse_server_example_2_0_0_data_second()).await;
    let (testing_config, event_stream_server_port, _temp_storage_dir) =
        build_testing_config_based_on_ports(vec![
            (sse_port_1, rest_port_1),
            (sse_port_2, rest_port_2),
        ]);
    start_sidecar(testing_config).await;
    let (join_handle, receiver) =
        fetch_data_from_endpoint("/events?start_from=0", event_stream_server_port).await;
    let receiver = wait_for_n_messages(2, receiver, Duration::from_secs(120)).await;
    mock_node_1.stop().await;
    mock_node_2.start().await;
    wait_for_n_messages(1, receiver, Duration::from_secs(120)).await;
    mock_node_2.stop().await;
    let events_received = tokio::join!(join_handle).0.unwrap();
    let length = events_received.len();
    assert_eq!(length, 3);
    assert!(events_received.first().unwrap().contains("\"2.0.0\""));
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
        build_2_0_0(sse_server_example_2_0_0_data()).await;
    let (sse_port_2, rest_port_2, mut mock_node_2) =
        build_2_0_0(sse_server_example_2_0_0_data_second()).await;
    start_nodes_and_wait(vec![&mut mock_node_1, &mut mock_node_2]).await;
    let (testing_config, event_stream_server_port, _temp_storage_dir) =
        build_testing_config_based_on_ports(vec![
            (sse_port_1, rest_port_1),
            (sse_port_2, rest_port_2),
        ]);
    start_sidecar(testing_config).await;
    let (join_handle, receiver) =
        fetch_data_from_endpoint("/events?start_from=0", event_stream_server_port).await;
    wait_for_n_messages(3, receiver, Duration::from_secs(120)).await;
    stop_nodes_and_wait(vec![&mut mock_node_1, &mut mock_node_2]).await;
    let events_received = tokio::join!(join_handle).0.unwrap();
    let length = events_received.len();
    assert_eq!(length, 3);
    assert!(events_received.first().unwrap().contains("\"2.0.0\""));
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
        build_2_0_0(sse_server_example_2_0_0_data()).await;
    let (sse_port_2, rest_port_2, mut mock_node_2) =
        build_2_0_0(sse_server_example_2_0_0_data_second()).await;
    start_nodes_and_wait(vec![&mut mock_node_1, &mut mock_node_2]).await;
    let (testing_config, event_stream_server_port, _temp_storage_dir) =
        build_testing_config_based_on_ports(vec![
            (sse_port_1, rest_port_1),
            (sse_port_2, rest_port_2),
            (8888, 9999), //Ports which should be not occupied
        ]);
    start_sidecar(testing_config).await;
    let (join_handle, receiver) =
        fetch_data_from_endpoint("/events?start_from=0", event_stream_server_port).await;
    wait_for_n_messages(3, receiver, Duration::from_secs(120)).await;
    stop_nodes_and_wait(vec![&mut mock_node_1, &mut mock_node_2]).await;

    let events_received = tokio::join!(join_handle).0.unwrap();
    let length = events_received.len();
    assert_eq!(length, 3);
    assert!(events_received.first().unwrap().contains("\"2.0.0\""));
    assert!(any_string_contains(
        &events_received,
        format!("\"{BLOCK_HASH_2}\"")
    ));
    assert!(any_string_contains(
        &events_received,
        format!("\"{BLOCK_HASH_3}\"")
    ));
}

pub async fn try_connect_to_single_stream(
    url: &str,
) -> Option<EventStream<impl Stream<Item = reqwest::Result<Bytes>> + Sized>> {
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
    event_stream
}

pub async fn poll_events<E, S>(mut stream: S, sender: mpsc::Sender<()>) -> Vec<String>
where
    E: Debug,
    S: Stream<Item = Result<Event, E>> + Sized + Unpin,
{
    let mut events_received = Vec::new();
    while let Some(res) = stream.next().await {
        match res {
            Ok(event) => {
                let _ = sender.send(()).await;
                events_received.push(event.data);
            }
            Err(_) => {
                break;
            }
        }
    }
    events_received
}
pub async fn fetch_data_from_endpoint(
    endpoint: &str,
    port: u16,
) -> (tokio::task::JoinHandle<Vec<String>>, mpsc::Receiver<()>) {
    fetch_data_from_endpoint_with_panic_flag(endpoint, port, true).await
}

pub async fn fetch_data_from_endpoint_with_panic_flag(
    endpoint: &str,
    port: u16,
    panic_on_cant_connect: bool,
) -> (tokio::task::JoinHandle<Vec<String>>, mpsc::Receiver<()>) {
    let local_endpoint = endpoint.to_owned();
    let (sender, receiver) = mpsc::channel(100);
    let join = tokio::spawn(async move {
        let main_event_stream_url = format!("http://127.0.0.1:{}{}", port, local_endpoint);
        let maybe_main_event_stream = try_connect_to_single_stream(&main_event_stream_url).await;
        if let Some(main_event_stream) = maybe_main_event_stream {
            poll_events(main_event_stream, sender).await
        } else {
            if panic_on_cant_connect {
                panic!("Unable to connect to stream")
            }
            vec![]
        }
    });
    (join, receiver)
}

pub async fn build_2_0_0(data_of_node: EventsWithIds) -> (u16, u16, MockNode) {
    let node_mock = MockNodeBuilder {
        version: "2.0.0".to_string(),
        network_name: "network-1".to_string(),
        data_of_node,
        cache_of_node: None,
        sse_port: None,
        rest_port: None,
    }
    .build();
    (
        node_mock.get_sse_port(),
        node_mock.get_rest_port(),
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
