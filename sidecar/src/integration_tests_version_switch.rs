#[cfg(test)]
pub mod tests {
    use crate::{
        integration_tests::{build_test_config, fetch_data_from_endpoint, start_sidecar},
        testing::{
            mock_node::tests::MockNodeBuilder,
            raw_sse_events_utils::tests::{
                example_data_1_0_0_two_blocks, example_data_1_1_0_with_legacy_message,
            },
        },
        utils::{start_nodes_and_wait, stop_nodes_and_wait, wait_for_n_messages},
    };
    use casper_event_types::sse_data::test_support::{BLOCK_HASH_1, BLOCK_HASH_2, BLOCK_HASH_3};
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn should_successfully_switch_api_versions() {
        let (
            testing_config,
            _temp_storage_dir,
            node_port_for_sse_connection,
            node_port_for_rest_connection,
            event_stream_server_port,
        ) = build_test_config();
        let mut node_mock = MockNodeBuilder::example_1_0_0_node(
            node_port_for_sse_connection,
            node_port_for_rest_connection,
        );
        start_nodes_and_wait(vec![&mut node_mock]).await;
        start_sidecar(testing_config).await;
        let (join_handle, receiver) =
            fetch_data_from_endpoint("/events/main?start_from=0", event_stream_server_port).await;
        let receiver = wait_for_n_messages(1, receiver, Duration::from_secs(120)).await;
        stop_nodes_and_wait(vec![&mut node_mock]).await;

        //At this point node 1.0.0 should be gone, set up 1.4.10 below
        let mut node_mock = MockNodeBuilder::build_example_1_4_10_node(
            node_port_for_sse_connection,
            node_port_for_rest_connection,
        );
        start_nodes_and_wait(vec![&mut node_mock]).await;
        wait_for_n_messages(2, receiver, Duration::from_secs(120)).await;
        stop_nodes_and_wait(vec![&mut node_mock]).await;

        let events_received = tokio::join!(join_handle).0.unwrap();
        assert_eq!(events_received.len(), 4);
        assert!(events_received.get(0).unwrap().contains("\"1.0.0\""));
        //block hash for 1.0.0
        let block_entry_1_0_0 = events_received.get(1).unwrap();
        assert!(block_entry_1_0_0.contains(format!("\"{BLOCK_HASH_1}\"").as_str()));
        assert!(block_entry_1_0_0.contains("\"rewards\":{")); // rewards should be in 1.0.0 format
        assert!(events_received.get(2).unwrap().contains("\"1.4.10\""));
        //block hash for 1.4.10
        let block_entry_1_4_10 = events_received.get(3).unwrap();
        assert!(block_entry_1_4_10.contains(format!("\"{BLOCK_HASH_2}\"").as_str()));
        assert!(block_entry_1_4_10.contains("\"rewards\":[")); // rewards should be in 1.4.x format
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn should_deserialize_legacy_messages_based_on_status_build_version() {
        let (
            testing_config,
            _temp_storage_dir,
            node_port_for_sse_connection,
            node_port_for_rest_connection,
            event_stream_server_port,
        ) = build_test_config();
        let mut node_mock = MockNodeBuilder {
            version: "1.0.0".to_string(),
            data_of_node: example_data_1_0_0_two_blocks(),
            cache_of_node: None,
            sse_port: Some(node_port_for_sse_connection),
            rest_port: Some(node_port_for_rest_connection),
        }
        .build();
        start_nodes_and_wait(vec![&mut node_mock]).await;
        start_sidecar(testing_config).await;
        let (join_handle, receiver) =
            fetch_data_from_endpoint("/events/main?start_from=0", event_stream_server_port).await;
        let receiver = wait_for_n_messages(2, receiver, Duration::from_secs(120)).await;
        stop_nodes_and_wait(vec![&mut node_mock]).await;

        //At this point node 1.0.0 should be gone, set up 1.1.0 below
        let mut node_mock = MockNodeBuilder {
            version: "1.1.0".to_string(),
            data_of_node: example_data_1_1_0_with_legacy_message(),
            cache_of_node: None,
            sse_port: Some(node_port_for_sse_connection),
            rest_port: Some(node_port_for_rest_connection),
        }
        .build();
        start_nodes_and_wait(vec![&mut node_mock]).await;
        wait_for_n_messages(3, receiver, Duration::from_secs(120)).await;
        stop_nodes_and_wait(vec![&mut node_mock]).await;
        let events_received = tokio::join!(join_handle).0.unwrap();
        assert_eq!(events_received.len(), 5);
        assert!(events_received.get(0).unwrap().contains("\"1.0.0\""));
        //block hash for 1.0.0
        let block_entry_1_0_0 = events_received.get(1).unwrap();
        assert!(block_entry_1_0_0.contains(format!("\"{BLOCK_HASH_1}\"").as_str()));
        assert!(block_entry_1_0_0.contains("\"rewards\":{")); // rewards should be in 1.0.0 format
        let block_entry_1_0_0 = events_received.get(2).unwrap();
        assert!(block_entry_1_0_0.contains(format!("\"{BLOCK_HASH_3}\"").as_str()));
        assert!(block_entry_1_0_0.contains("\"rewards\":{")); // rewards should be in 1.0.0 format
        assert!(events_received.get(3).unwrap().contains("\"1.1.0\""));
        //block hash for 1.4.10
        let second_block_entry = events_received.get(4).unwrap();
        assert!(second_block_entry.contains(format!("\"{BLOCK_HASH_2}\"").as_str()));
        assert!(second_block_entry.contains("\"rewards\":{")); // rewards should be in 1.0.0 format
    }
}
