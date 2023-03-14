#[cfg(test)]
mod tests {
    use core::time;
    use futures_util::StreamExt;
    use std::thread;
    use tempfile::tempdir;

    use crate::integration_tests::try_connect_to_single_stream;
    use crate::run;
    use crate::testing::fake_event_stream::setup_mock_build_version_server_with_version;
    use crate::testing::simple_sse_server::tests::*;
    use crate::testing::testing_config::prepare_config;
    use casper_event_types::sse_data::test_support::{BLOCK_HASH_1, BLOCK_HASH_2};

    async fn start_sidecar() -> (u16, u16, u16) {
        let temp_storage_dir =
            tempdir().expect("Should have created a temporary storage directory");
        let mut testing_config = prepare_config(&temp_storage_dir);
        testing_config.add_connection(None, None, None);
        let node_port_for_sse_connection =
            testing_config.config.connections.get(0).unwrap().sse_port;
        let node_port_for_rest_connection =
            testing_config.config.connections.get(0).unwrap().rest_port;
        testing_config.set_retries_for_node(node_port_for_sse_connection, 2, 1);
        testing_config.set_allow_partial_connection_for_node(node_port_for_sse_connection, true);
        tokio::spawn(run(testing_config.inner())); // starting event sidecar
        (
            node_port_for_sse_connection,
            node_port_for_rest_connection,
            testing_config.event_stream_server_port(),
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn should_successfully_switch_api_versions() {
        let (node_port_for_sse_connection, node_port_for_rest_connection, event_stream_server_port) =
            start_sidecar().await;
        let shutdown_tx = sse_server_example_data_1_0_0(node_port_for_sse_connection).await;
        let change_api_version_tx = setup_mock_build_version_server_with_version(
            node_port_for_rest_connection,
            "1.0.0".to_string(),
        );
        let main_event_stream_url = format!(
            "http://127.0.0.1:{}/events/main?start_from=0",
            event_stream_server_port
        );
        let mut main_event_stream = try_connect_to_single_stream(&main_event_stream_url).await;
        shutdown_tx.send(()).unwrap();
        change_api_version_tx
            .send("1.4.10".to_string())
            .await
            .unwrap();
        thread::sleep(time::Duration::from_secs(1)); //give some time everything to disconnect
        let shutdown_tx = sse_server_example_data_1_4_10(node_port_for_sse_connection).await;
        thread::sleep(time::Duration::from_secs(3)); //give some time for sidecar to connect and read data
        shutdown_tx.send(()).unwrap();
        let mut events_received = Vec::new();
        while let Some(Ok(event)) = main_event_stream.next().await {
            events_received.push(event.data);
        }
        assert_eq!(events_received.len(), 5);
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
        assert!(events_received.get(4).unwrap().contains("\"Shutdown\""));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn polling_latest_data_should_return_latest_api_version() {
        let (node_port_for_sse_connection, node_port_for_rest_connection, event_stream_server_port) =
            start_sidecar().await;
        let shutdown_tx = sse_server_example_data_1_0_0(node_port_for_sse_connection).await;
        let change_api_version_tx = setup_mock_build_version_server_with_version(
            node_port_for_rest_connection,
            "1.0.0".to_string(),
        );
        shutdown_tx.send(()).unwrap();
        change_api_version_tx
            .send("1.4.10".to_string())
            .await
            .unwrap();
        let main_event_stream_url =
            format!("http://127.0.0.1:{}/events/main", event_stream_server_port);
        let mut main_event_stream = try_connect_to_single_stream(&main_event_stream_url).await;
        let shutdown_tx = sse_server_example_data_1_4_10(node_port_for_sse_connection).await;
        thread::sleep(time::Duration::from_secs(3)); //give some time everything to disconnect
        shutdown_tx.send(()).unwrap();
        let mut events_received = Vec::new();
        while let Some(Ok(event)) = main_event_stream.next().await {
            events_received.push(event.data);
        }
        assert_eq!(events_received.len(), 3);
        assert!(events_received.get(0).unwrap().contains("\"1.4.10\""));
        //block hash for 1.4.10
        let block_entry_1_4_10 = events_received.get(1).unwrap();
        assert!(block_entry_1_4_10.contains(format!("\"{BLOCK_HASH_2}\"").as_str()));
        assert!(block_entry_1_4_10.contains("\"rewards\":[")); // rewards should be in 1.4.x format
        assert!(events_received.get(2).unwrap().contains("\"Shutdown\""));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn should_deserialize_legacy_messages_based_on_status_build_version() {
        let (node_port_for_sse_connection, node_port_for_rest_connection, event_stream_server_port) =
            start_sidecar().await;
        let shutdown_tx = sse_server_example_data_1_0_0(node_port_for_sse_connection).await;
        let change_api_version_tx = setup_mock_build_version_server_with_version(
            node_port_for_rest_connection,
            "1.0.0".to_string(),
        );
        let main_event_stream_url = format!(
            "http://127.0.0.1:{}/events/main?start_from=0",
            event_stream_server_port
        );
        let mut main_event_stream = try_connect_to_single_stream(&main_event_stream_url).await;
        change_api_version_tx
            .send("1.1.0".to_string()) //1.1.0 is different but still has the old message format
            .await
            .unwrap();
        shutdown_tx.send(()).unwrap();
        thread::sleep(time::Duration::from_secs(1)); //give some time everything to disconnect
        let shutdown_tx =
            sse_server_example_data_1_1_0_with_legacy_message(node_port_for_sse_connection).await;
        thread::sleep(time::Duration::from_secs(3)); //give some time for sidecar to connect and read data
        shutdown_tx.send(()).unwrap();
        let mut events_received = Vec::new();
        while let Some(Ok(event)) = main_event_stream.next().await {
            events_received.push(event.data);
        }
        assert_eq!(events_received.len(), 5);
        assert!(events_received.get(0).unwrap().contains("\"1.0.0\""));
        //block hash for 1.0.0
        let block_entry_1_0_0 = events_received.get(1).unwrap();
        assert!(block_entry_1_0_0.contains(format!("\"{BLOCK_HASH_1}\"").as_str()));
        assert!(block_entry_1_0_0.contains("\"rewards\":{")); // rewards should be in 1.0.0 format
        assert!(events_received.get(2).unwrap().contains("\"1.1.0\""));
        //block hash for 1.4.10
        let second_block_entry = events_received.get(3).unwrap();
        assert!(second_block_entry.contains(format!("\"{BLOCK_HASH_2}\"").as_str()));
        assert!(second_block_entry.contains("\"rewards\":{")); // rewards should be in 1.0.0 format
        assert!(events_received.get(4).unwrap().contains("\"Shutdown\""));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn should_handle_node_changing_filters() {
        let (node_port_for_sse_connection, node_port_for_rest_connection, event_stream_server_port) =
            start_sidecar().await;
        let shutdown_tx = sse_server_example_data_1_0_0(node_port_for_sse_connection).await;
        let change_api_version_tx = setup_mock_build_version_server_with_version(
            node_port_for_rest_connection,
            "1.0.0".to_string(),
        );
        let main_event_stream_url = format!(
            "http://127.0.0.1:{}/events/sigs?start_from=0",
            event_stream_server_port
        );
        let mut main_event_stream = try_connect_to_single_stream(&main_event_stream_url).await;

        change_api_version_tx
            .send("1.3.9".to_string()) //1.3.x shold have /main and /sigs
            .await
            .unwrap();
        shutdown_tx.send(()).unwrap();
        thread::sleep(time::Duration::from_secs(1)); //give some time everything to disconnect
        let shutdown_tx =
            sse_server_example_data_1_3_9_with_sigs(node_port_for_sse_connection).await;
        thread::sleep(time::Duration::from_secs(3)); //give some time for sidecar to connect and read data
        shutdown_tx.send(()).unwrap();
        let mut events_received = Vec::new();
        while let Some(Ok(event)) = main_event_stream.next().await {
            events_received.push(event.data);
        }
        assert_eq!(events_received.len(), 4);
        assert!(events_received.get(0).unwrap().contains("\"1.0.0\""));
        //there should be no messages for 1.0.0
        assert!(events_received.get(1).unwrap().contains("\"1.3.9\""));
        //finality sigmature for 1.3.9
        let finality_signature = events_received.get(2).unwrap(); //Should read finality
                                                                  //signature from /main/sigs of mock node
        assert!(finality_signature.contains("\"FinalitySignature\""));
        assert!(finality_signature.contains(format!("\"{BLOCK_HASH_2}\"").as_str()));
        assert!(events_received.get(3).unwrap().contains("\"Shutdown\""));
    }
}
