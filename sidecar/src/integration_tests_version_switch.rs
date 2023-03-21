#[cfg(test)]
pub mod tests {
    use crate::{
        integration_tests::{connect_to_sidecar, poll_events, start_sidecar},
        testing::{
            fake_event_stream::status_1_0_0_server,
            simple_sse_server::tests::{
                sse_server_example_data_1_0_0, sse_server_example_data_1_1_0_with_legacy_message,
                sse_server_example_data_1_3_9_with_sigs, sse_server_example_data_1_4_10,
            },
        },
    };
    use casper_event_types::sse_data::test_support::{BLOCK_HASH_1, BLOCK_HASH_2};
    use core::time;
    use std::thread;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn should_successfully_switch_api_versions() {
        let (node_port_for_sse_connection, node_port_for_rest_connection, event_stream_server_port) =
            start_sidecar().await;
        let shutdown_tx = sse_server_example_data_1_0_0(node_port_for_sse_connection).await;
        let change_api_version_tx = status_1_0_0_server(node_port_for_rest_connection);
        thread::sleep(time::Duration::from_secs(5)); //give some time everything to connect
        let main_event_stream =
            connect_to_sidecar("/events/main?start_from=0", event_stream_server_port).await;
        shutdown_tx.send(()).unwrap();
        change_api_version_tx
            .send("1.4.10".to_string())
            .await
            .unwrap();
        thread::sleep(time::Duration::from_secs(2)); //give some time everything to disconnect
        let shutdown_tx = sse_server_example_data_1_4_10(node_port_for_sse_connection).await;
        thread::sleep(time::Duration::from_secs(5)); //give some time for sidecar to connect and read data
        shutdown_tx.send(()).unwrap();

        let events_received = poll_events(main_event_stream).await;
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
        let change_api_version_tx = status_1_0_0_server(node_port_for_rest_connection);
        change_api_version_tx
            .send("1.4.10".to_string())
            .await
            .unwrap();
        shutdown_tx.send(()).unwrap();
        thread::sleep(time::Duration::from_secs(3)); //give some time everything to disconnect
        let main_event_stream = connect_to_sidecar("/events/main", event_stream_server_port).await;
        let shutdown_tx = sse_server_example_data_1_4_10(node_port_for_sse_connection).await;
        tokio::spawn(async move{
            thread::sleep(time::Duration::from_secs(4)); //give some time to poll the data
            shutdown_tx.send(()).unwrap();
        });
        let events_received = poll_events(main_event_stream).await;
        assert_eq!(events_received.len(), 3);
        assert!(events_received.get(0).unwrap().contains("\"1.4.10\""));
        //block hash for 1.4.10
        let block_entry_1_4_10 = events_received.get(1).unwrap();
        assert!(block_entry_1_4_10.contains(format!("\"{BLOCK_HASH_2}\"").as_str()));
        assert!(block_entry_1_4_10.contains("\"rewards\":[")); // rewards should be in 1.4.x format
        assert!(events_received.get(2).unwrap().contains("\"Shutdown\""));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn should_deserialize_legacy_messages_based_on_status_build_version() {
        let (node_port_for_sse_connection, node_port_for_rest_connection, event_stream_server_port) =
            start_sidecar().await;
        let shutdown_tx = sse_server_example_data_1_0_0(node_port_for_sse_connection).await;
        let change_api_version_tx = status_1_0_0_server(node_port_for_rest_connection);
        thread::sleep(time::Duration::from_secs(3)); //give some time everything to connect
        let main_event_stream =
            connect_to_sidecar("/events/main?start_from=0", event_stream_server_port).await;
        change_api_version_tx
            .send("1.1.0".to_string()) //1.1.0 is different but still has the old message format
            .await
            .unwrap();
        shutdown_tx.send(()).unwrap();
        thread::sleep(time::Duration::from_secs(3)); //give some time everything to disconnect
        let shutdown_tx =
            sse_server_example_data_1_1_0_with_legacy_message(node_port_for_sse_connection).await;
        thread::sleep(time::Duration::from_secs(5)); //give some time for sidecar to connect and read data
        shutdown_tx.send(()).unwrap();
        let events_received = poll_events(main_event_stream).await;
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn should_handle_node_changing_filters() {
        let (node_port_for_sse_connection, node_port_for_rest_connection, event_stream_server_port) =
            start_sidecar().await;
        let shutdown_tx = sse_server_example_data_1_0_0(node_port_for_sse_connection).await;
        let change_api_version_tx = status_1_0_0_server(node_port_for_rest_connection);
        thread::sleep(time::Duration::from_secs(5)); //give some time for sidecar to connect and read data
        let main_event_stream =
            connect_to_sidecar("/events/sigs?start_from=0", event_stream_server_port).await;
        change_api_version_tx
            .send("1.3.9".to_string()) //1.3.x shold have /main and /sigs
            .await
            .unwrap();
        thread::sleep(time::Duration::from_secs(3)); //give some time for sidecar to connect and data to propagate
        shutdown_tx.send(()).unwrap();
        thread::sleep(time::Duration::from_secs(3)); //give some time everything to disconnect
        let shutdown_tx =
            sse_server_example_data_1_3_9_with_sigs(node_port_for_sse_connection).await;
        thread::sleep(time::Duration::from_secs(5)); //give some time for sidecar to connect and read data
        shutdown_tx.send(()).unwrap();
        let events_received = poll_events(main_event_stream).await;
        assert_eq!(events_received.len(), 4);
        assert!(events_received.get(0).unwrap().contains("\"1.0.0\""));
        //there should be no messages for 1.0.0
        assert!(events_received.get(1).unwrap().contains("\"1.3.9\""));
        //finality sigmature for 1.3.9
        let finality_signature = events_received.get(2).unwrap(); //Should read finality signature from /main/sigs of mock node
        assert!(finality_signature.contains("\"FinalitySignature\""));
        assert!(finality_signature.contains(format!("\"{BLOCK_HASH_2}\"").as_str()));
        assert!(events_received.get(3).unwrap().contains("\"Shutdown\""));
    }
}
