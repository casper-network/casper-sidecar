#[cfg(test)]
pub mod tests {

    use futures::join;
    use tokio::sync::mpsc::{Receiver, Sender};
    use tokio::task::JoinHandle;

    use crate::testing::fake_event_stream::setup_mock_build_version_server_with_version;
    use crate::testing::simple_sse_server::tests::{
        simple_sse_server, simple_sse_server_with_sigs,
    };

    pub struct MockNode {
        sse_server_shutdown_tx: Sender<()>,
        sse_server_after_shutdown_receiver_tx: Receiver<()>,
        rest_server_shutdown_tx: Sender<()>,
        rest_server_after_shutdown_receiver_tx: Receiver<()>,
    }

    impl MockNode {
        pub async fn new_with_sigs(
            version: String,
            main_and_sigs: (Vec<(Option<String>, String)>, Vec<(Option<String>, String)>),
            sse_port: u16,
            rest_port: u16,
        ) -> Self {
            let main_clone = main_and_sigs.0.clone();
            let sigs_clone = main_and_sigs.1.clone();
            let spin_up_sse_server = |port| {
                tokio::spawn(async move {
                    simple_sse_server_with_sigs(port, main_clone, sigs_clone).await
                })
            };
            Self::build_mock(version, sse_port, rest_port, Box::new(spin_up_sse_server)).await
        }

        pub async fn new(
            version: String,
            data_of_node: Vec<(Option<String>, String)>,
            sse_port: u16,
            rest_port: u16,
        ) -> Self {
            let data_of_node_clone = data_of_node.clone();
            let spin_up_sse_server = |port| {
                tokio::spawn(async move { simple_sse_server(port, data_of_node_clone).await })
            };
            Self::build_mock(version, sse_port, rest_port, Box::new(spin_up_sse_server)).await
        }

        pub async fn stop(&mut self) {
            self.sse_server_shutdown_tx.send(()).await.unwrap();
            self.rest_server_shutdown_tx.send(()).await.unwrap();
            self.sse_server_after_shutdown_receiver_tx
                .recv()
                .await
                .unwrap();
            self.rest_server_after_shutdown_receiver_tx
                .recv()
                .await
                .unwrap();
        }

        async fn build_mock(
            version: String,
            sse_port: u16,
            rest_port: u16,
            spin_up_sse_server: Box<dyn FnOnce(u16) -> JoinHandle<(Sender<()>, Receiver<()>)>>,
        ) -> Self {
            let version_clone = version.clone();
            let sse_server_join: JoinHandle<(Sender<()>, Receiver<()>)> =
                spin_up_sse_server(sse_port);
            let rest_server_join = tokio::spawn(async move {
                setup_mock_build_version_server_with_version(rest_port, version_clone).await
            });
            let sse_and_rest_joins = join!(sse_server_join, rest_server_join);
            let (sse_server_shutdown_tx, sse_server_after_shutdown_receiver_tx) =
                sse_and_rest_joins.0.unwrap();
            let (rest_server_shutdown_tx, rest_server_after_shutdown_receiver_tx) =
                sse_and_rest_joins.1.unwrap();
            MockNode {
                sse_server_shutdown_tx,
                sse_server_after_shutdown_receiver_tx,
                rest_server_shutdown_tx,
                rest_server_after_shutdown_receiver_tx,
            }
        }
    }
}
