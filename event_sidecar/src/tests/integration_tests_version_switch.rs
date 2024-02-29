#[cfg(test)]
pub mod tests {
    use crate::{
        testing::mock_node::tests::MockNodeBuilder,
        tests::integration_tests::fetch_data_from_endpoint,
        utils::tests::{
            prepare_one_node_and_start, start_nodes_and_wait, stop_nodes_and_wait,
            wait_for_n_messages,
        },
    };
    use casper_event_types::sse_data::test_support::{BLOCK_HASH_2, BLOCK_HASH_3};
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn should_successfully_switch_api_versions() {
        let mut node_mock =
            MockNodeBuilder::build_example_node_with_version(None, None, "2.0.0", "network-1");
        let properties = prepare_one_node_and_start(&mut node_mock).await;
        let (join_handle, receiver) =
            fetch_data_from_endpoint("/events?start_from=0", properties.event_stream_server_port)
                .await;
        let receiver = wait_for_n_messages(1, receiver, Duration::from_secs(120)).await;
        stop_nodes_and_wait(vec![&mut node_mock]).await;

        //At this point node 2.0.0 should be gone, set up 2.0.1 below
        let mut node_mock = MockNodeBuilder::build_example_2_0_1_node(
            properties.node_port_for_sse_connection,
            properties.node_port_for_rest_connection,
        );
        start_nodes_and_wait(vec![&mut node_mock]).await;
        wait_for_n_messages(2, receiver, Duration::from_secs(120)).await;
        stop_nodes_and_wait(vec![&mut node_mock]).await;

        let events_received = tokio::join!(join_handle).0.unwrap();
        assert_eq!(events_received.len(), 4);
        assert!(events_received.first().unwrap().contains("\"2.0.0\""));
        //block hash for 2.0.0
        let block_entry_2_0_0 = events_received.get(1).unwrap();
        assert!(block_entry_2_0_0.contains(format!("\"{BLOCK_HASH_2}\"").as_str()));
        assert!(events_received.get(2).unwrap().contains("\"2.0.1\""));
        //block hash for 2.0.1
        let block_entry_2_0_1 = events_received.get(3).unwrap();
        assert!(block_entry_2_0_1.contains(format!("\"{BLOCK_HASH_3}\"").as_str()));
    }
}
