#[cfg(test)]
pub mod tests {
    use crate::{
        integration_tests::{build_test_config, fetch_data_from_endpoint, start_sidecar},
        testing::{
            mock_node::tests::MockNode,
            raw_sse_events_utils::tests::{
                example_data_1_0_0, example_data_1_0_0_two_blocks,
                example_data_1_1_0_with_legacy_message, example_data_1_3_9_with_sigs,
                sse_server_example_1_4_10_data,
            },
        },
    };
    use casper_event_types::sse_data::test_support::{BLOCK_HASH_1, BLOCK_HASH_2, BLOCK_HASH_3};
    use core::time;
    use std::thread;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn should_successfully_switch_api_versions() {
        let (
            testing_config,
            _temp_storage_dir,
            node_port_for_sse_connection,
            node_port_for_rest_connection,
            event_stream_server_port,
        ) = build_test_config();
        let mut node_mock = MockNode::new(
            "1.0.0".to_string(),
            example_data_1_0_0(),
            node_port_for_sse_connection,
            node_port_for_rest_connection,
        )
        .await;
        node_mock.start().await;
        start_sidecar(testing_config).await;
        let (join_handle, mut receiver) =
            fetch_data_from_endpoint("/events/main?start_from=0", event_stream_server_port).await;
        receiver.recv().await.unwrap();
        node_mock.stop().await;
        let mut node_mock = MockNode::new(
            "1.4.10".to_string(),
            sse_server_example_1_4_10_data(),
            node_port_for_sse_connection,
            node_port_for_rest_connection,
        )
        .await;
        node_mock.start().await;
        thread::sleep(time::Duration::from_secs(5)); //give some time for sidecar to connect and read data
        node_mock.stop().await;

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
        let mut node_mock = MockNode::new(
            "1.0.0".to_string(),
            example_data_1_0_0_two_blocks(),
            node_port_for_sse_connection,
            node_port_for_rest_connection,
        )
        .await;
        node_mock.start().await;
        start_sidecar(testing_config).await;
        let (join_handle, mut receiver) =
            fetch_data_from_endpoint("/events/main?start_from=0", event_stream_server_port).await;
        receiver.recv().await.unwrap();
        node_mock.stop().await;

        let mut node_mock = MockNode::new(
            "1.1.0".to_string(),
            example_data_1_1_0_with_legacy_message(),
            node_port_for_sse_connection,
            node_port_for_rest_connection,
        )
        .await;
        node_mock.start().await;
        thread::sleep(time::Duration::from_secs(5)); //give some time for sidecar to connect and read data
        node_mock.stop().await;
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn should_handle_node_changing_filters() {
        let (
            testing_config,
            _temp_storage_dir,
            node_port_for_sse_connection,
            node_port_for_rest_connection,
            event_stream_server_port,
        ) = build_test_config();
        let mut node_mock = MockNode::new(
            "1.0.0".to_string(),
            example_data_1_0_0(),
            node_port_for_sse_connection,
            node_port_for_rest_connection,
        )
        .await;
        node_mock.start().await;
        let (join_handle, mut receiver) =
            fetch_data_from_endpoint("/events/sigs?start_from=0", event_stream_server_port).await;
        start_sidecar(testing_config).await;
        node_mock.stop().await;
        let mut node_mock = MockNode::new_with_sigs(
            "1.3.9".to_string(),
            example_data_1_3_9_with_sigs(),
            node_port_for_sse_connection,
            node_port_for_rest_connection,
        )
        .await;
        node_mock.start().await;
        receiver.recv().await.unwrap(); // Wait for the first event to go through to outbound
        thread::sleep(time::Duration::from_secs(3)); //give some time for sidecar to connect and read data
        node_mock.stop().await;
        let events_received = tokio::join!(join_handle).0.unwrap();
        assert_eq!(events_received.len(), 2);
        //there should be no messages for 1.0.0
        assert!(events_received.get(0).unwrap().contains("\"1.3.9\""));
        //finality sigmature for 1.3.9
        let finality_signature = events_received.get(1).unwrap(); //Should read finality signature from /main/sigs of mock node
        assert!(finality_signature.contains("\"FinalitySignature\""));
        assert!(finality_signature.contains(format!("\"{BLOCK_HASH_2}\"").as_str()));
    }
}
