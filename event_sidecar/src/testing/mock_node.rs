#[cfg(test)]
pub mod tests {
    use std::time::Duration;

    use crate::testing::fake_event_stream::setup_mock_build_version_server_with_version;
    use crate::testing::raw_sse_events_utils::tests::{
        example_data_2_0_1, simple_sse_server, sse_server_example_data, EventsWithIds,
    };
    use crate::testing::testing_config::get_port;
    use futures::join;
    use tokio::sync::mpsc::{Receiver, Sender};
    use tokio::task::JoinHandle;

    pub struct MockNodeBuilder {
        pub version: String,
        pub network_name: String,
        pub data_of_node: EventsWithIds,
        pub cache_of_node: Option<EventsWithIds>,
        pub sse_port: Option<u16>,
        pub rest_port: Option<u16>,
    }

    impl MockNodeBuilder {
        pub fn build_example_2_0_1_node(
            node_port_for_sse_connection: u16,
            node_port_for_rest_connection: u16,
        ) -> MockNode {
            MockNodeBuilder {
                version: "2.0.1".to_string(),
                network_name: "network1".to_string(),
                data_of_node: example_data_2_0_1(),
                cache_of_node: None,
                sse_port: Some(node_port_for_sse_connection),
                rest_port: Some(node_port_for_rest_connection),
            }
            .build()
        }

        pub fn build_example_2_0_0_node(
            node_port_for_sse_connection: u16,
            node_port_for_rest_connection: u16,
        ) -> MockNode {
            Self::build_example_node_with_version(
                Some(node_port_for_sse_connection),
                Some(node_port_for_rest_connection),
                "2.0.0",
                "network1",
            )
        }

        pub fn build_example_2_0_0_node_with_data(
            node_port_for_sse_connection: u16,
            node_port_for_rest_connection: u16,
            data: EventsWithIds,
        ) -> MockNode {
            MockNodeBuilder {
                version: "2.0.0".to_string(),
                network_name: "network-1".to_string(),
                data_of_node: data,
                cache_of_node: None,
                sse_port: Some(node_port_for_sse_connection),
                rest_port: Some(node_port_for_rest_connection),
            }
            .build()
        }

        pub fn build_example_node_with_version(
            node_port_for_sse_connection: Option<u16>,
            node_port_for_rest_connection: Option<u16>,
            version: &str,
            network_name: &str,
        ) -> MockNode {
            MockNodeBuilder {
                version: version.to_string(),
                network_name: network_name.to_string(),
                data_of_node: sse_server_example_data(version),
                cache_of_node: None,
                sse_port: node_port_for_sse_connection,
                rest_port: node_port_for_rest_connection,
            }
            .build()
        }
        pub fn build(&self) -> MockNode {
            let sse_port = self.sse_port.unwrap_or(get_port());
            let rest_port = self.rest_port.unwrap_or(get_port());
            let cache_of_node = self.cache_of_node.clone().unwrap_or_default();
            MockNode::new(
                self.version.clone(),
                self.data_of_node.clone(),
                cache_of_node,
                sse_port,
                rest_port,
                self.network_name.clone(),
            )
        }
    }

    pub struct MockNode {
        version: String,
        network_name: String,
        data_of_node: EventsWithIds,
        cache_of_node: EventsWithIds,
        sse_port: u16,
        rest_port: u16,
        sse_server_shutdown_tx: Option<Sender<()>>,
        sse_server_after_shutdown_receiver_rx: Option<Receiver<()>>,
        rest_server_shutdown_tx: Option<Sender<()>>,
        rest_server_after_shutdown_receiver_rx: Option<Receiver<()>>,
    }

    impl MockNode {
        pub fn get_sse_port(&self) -> u16 {
            self.sse_port
        }

        pub fn get_rest_port(&self) -> u16 {
            self.rest_port
        }

        pub fn set_sse_port(&mut self, port: u16) {
            self.sse_port = port
        }

        pub fn set_rest_port(&mut self, port: u16) {
            self.rest_port = port
        }

        fn new(
            version: String,
            data_of_node: EventsWithIds,
            cache_of_node: EventsWithIds,
            sse_port: u16,
            rest_port: u16,
            network_name: String,
        ) -> MockNode {
            MockNode {
                version,
                network_name,
                data_of_node,
                cache_of_node,
                sse_port,
                rest_port,
                sse_server_shutdown_tx: None,
                sse_server_after_shutdown_receiver_rx: None,
                rest_server_shutdown_tx: None,
                rest_server_after_shutdown_receiver_rx: None,
            }
        }

        pub async fn start(&mut self) {
            let data_of_node_clone = self.data_of_node.clone();
            let cache_of_node = self.cache_of_node.clone();
            let version = self.version.clone();
            let network_name = self.network_name.clone();
            let sse_port = self.sse_port;
            let rest_port = self.rest_port;

            //Spin up sse server
            let sse_server_join: JoinHandle<(Sender<()>, Receiver<()>, Vec<String>)> =
                tokio::spawn(async move {
                    simple_sse_server(sse_port, data_of_node_clone, cache_of_node).await
                });
            //Spin up rest server
            let rest_server_join = tokio::spawn(async move {
                setup_mock_build_version_server_with_version(rest_port, version, network_name).await
            });
            //Get handles to stop the above servers, store them in the structure
            let sse_and_rest_joins = join!(sse_server_join, rest_server_join);
            let (sse_server_shutdown_tx, sse_server_after_shutdown_receiver_rx, urls) =
                sse_and_rest_joins.0.unwrap();
            let (rest_server_shutdown_tx, rest_server_after_shutdown_receiver_rx) =
                sse_and_rest_joins.1.unwrap();
            self.sse_server_shutdown_tx = Some(sse_server_shutdown_tx);
            self.sse_server_after_shutdown_receiver_rx =
                Some(sse_server_after_shutdown_receiver_rx);
            self.rest_server_shutdown_tx = Some(rest_server_shutdown_tx);
            self.rest_server_after_shutdown_receiver_rx =
                Some(rest_server_after_shutdown_receiver_rx);

            //Need to wait for the sse endpoints to be up
            wait_for_sse_server_to_be_up(urls).await;
        }

        pub async fn stop(&mut self) {
            if let Some(sse_server_shutdown_tx) = self.sse_server_shutdown_tx.take() {
                let _ = sse_server_shutdown_tx.send(()).await;
            }
            if let Some(rest_server_shutdown_tx) = self.rest_server_shutdown_tx.take() {
                let _ = rest_server_shutdown_tx.send(()).await;
            }
            if let Some(mut sse_server_after_shutdown_receiver_rx) =
                self.sse_server_after_shutdown_receiver_rx.take()
            {
                let _ = sse_server_after_shutdown_receiver_rx.recv().await;
            }
            if let Some(mut rest_server_after_shutdown_receiver_rx) =
                self.rest_server_after_shutdown_receiver_rx.take()
            {
                let _ = rest_server_after_shutdown_receiver_rx.recv().await;
            }
        }
    }

    pub async fn wait_for_sse_server_to_be_up(urls: Vec<String>) {
        let join_handles: Vec<JoinHandle<bool>> = urls
            .clone()
            .into_iter()
            .map(|url| {
                tokio::spawn(async move {
                    for _ in 0..10 {
                        let event_source = reqwest::Client::new().get(url.as_str()).send().await;
                        if event_source.is_ok() {
                            return true;
                        } else {
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                    false
                })
            })
            .collect();
        let result = futures::future::join_all(join_handles).await;
        let potential_error_message = format!(
            "Sse server didn't start for urls: {:?}, return values: {:?}",
            urls, result
        );
        if !result.into_iter().all(|elem| elem.is_ok() && elem.unwrap()) {
            panic!("{}", potential_error_message);
        }
    }
}
