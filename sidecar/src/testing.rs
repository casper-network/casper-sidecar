pub(crate) mod fake_database;
pub(crate) mod fake_event_stream;
pub(crate) mod test_clock;
mod test_rng;

use crate::types::config::Config;

struct TestingConfig {
    config: Config,
}

impl TestingConfig {
    fn default() -> Self {
        let config = Config::default();

        Self { config }
    }

    fn set_storage_path(mut self, path: String) -> Self {
        self.config.storage.storage_path = path;
        self
    }

    fn set_node_connection_port(mut self, port: u16) -> Self {
        self.config.connection.node_connections[0].sse_port = port;
        self
    }

    fn set_max_sse_subscribers(mut self, num_subscribers: u32) -> Self {
        self.config.event_stream_server.max_concurrent_subscribers = num_subscribers;
        self
    }

    fn configure_retry_settings(mut self, max_retries: u8, delay_between_retries: u8) -> Self {
        for connection in &mut self.config.connection.node_connections {
            connection.max_retries = max_retries;
            connection.delay_between_retries_in_seconds = delay_between_retries;
        }
        self
    }

    fn use_free_ports_for_servers(mut self) -> Self {
        let rest_server_port = portpicker::pick_unused_port().expect("Error getting free port");
        let sse_server_port = portpicker::pick_unused_port().expect("Error getting free port");
        self.config.rest_server.port = rest_server_port;
        self.config.event_stream_server.port = sse_server_port;

        self
    }

    fn inner(&self) -> Config {
        self.config.clone()
    }

    fn connection_port(&self) -> u16 {
        self.config.connection.node_connections[0].sse_port
    }

    fn rest_server_port(&self) -> u16 {
        self.config.rest_server.port
    }

    fn event_stream_server_port(&self) -> u16 {
        self.config.event_stream_server.port
    }
}

#[cfg(test)]
fn prepare_config(temp_storage: &tempfile::TempDir) -> TestingConfig {
    let path_to_temp_storage = temp_storage
        .path()
        .to_str()
        .expect("Error getting path of temporary directory")
        .to_string();

    // Get an unused port to bind the mock event_stream_server to
    let port_for_mock_event_stream =
        portpicker::pick_unused_port().expect("Unable to get free port");

    // test_config points to the mock_node
    TestingConfig::default()
        .set_storage_path(path_to_temp_storage)
        .set_node_connection_port(port_for_mock_event_stream)
        .use_free_ports_for_servers()
}
