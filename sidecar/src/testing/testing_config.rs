use portpicker::Port;
use tempfile::TempDir;

use crate::types::config::{Config, Connection};

/// A basic wrapper with helper methods for constructing and tweaking [Config]s for use in tests.
pub(crate) struct TestingConfig {
    config: Config,
}

/// Prepares an instance of [TestingConfig]. The instance has default values except:
/// - `storage_path` is set to the path of the [TempDir] provided.
/// - `node_connection_port` is set dynamically to a free port.
/// - The outbound server (REST & SSE) ports are set dynamically to free ports.
#[cfg(test)]
pub(crate) fn prepare_config(temp_storage: &TempDir) -> TestingConfig {
    let path_to_temp_storage = temp_storage.path().to_string_lossy().to_string();

    let mut testing_config = TestingConfig::default();
    testing_config.set_storage_path(path_to_temp_storage);
    testing_config.allocate_available_ports();

    testing_config
}

impl TestingConfig {
    /// Creates a Default instance of TestingConfig which contains a Default instance of [Config]
    pub(crate) fn default() -> Self {
        let config = Config::default();

        Self { config }
    }

    /// Specify where test storage (database, sse cache) should be located.
    /// By default it is set to `/target/test_storage` however it is recommended to overwrite this with a `TempDir` path for testing purposes.
    pub(crate) fn set_storage_path(&mut self, path: String) {
        self.config.storage.storage_path = path;
    }

    pub(crate) fn add_connection(
        &mut self,
        ip_address: Option<String>,
        sse_port: Option<u16>,
        rest_port: Option<u16>,
    ) -> Port {
        let random_port_for_sse = portpicker::pick_unused_port().unwrap();
        let random_port_for_rest = portpicker::pick_unused_port().unwrap();
        let connection = Connection {
            ip_address: ip_address.unwrap_or("127.0.0.1".to_string()),
            sse_port: sse_port.unwrap_or(random_port_for_sse),
            rest_port: rest_port.unwrap_or(random_port_for_rest),
            max_retries: 0,
            delay_between_retries_in_seconds: 0,
            allow_partial_connection: false,
            enable_logging: false,
        };
        self.config.connections.push(connection);
        random_port_for_sse
    }

    /// Set how the sidecar should handle the case where it is only able to connect to one or two of the node's filters.
    pub(crate) fn set_allow_partial_connection_for_node(
        &mut self,
        port_of_node: u16,
        allow_partial_connection: bool,
    ) {
        for mut connection in &mut self.config.connections {
            if connection.sse_port == port_of_node {
                connection.allow_partial_connection = allow_partial_connection;
                break;
            }
        }
    }

    /// Specify the retry configuration settings. By default they are set as follows:
    /// - `max_retries`: 3
    /// - `delay_between_retries_in_seconds`: 5
    pub(crate) fn set_retries_for_node(
        &mut self,
        port_of_node: u16,
        max_retries: usize,
        delay_between_retries_in_seconds: usize,
    ) {
        for mut connection in &mut self.config.connections {
            if connection.sse_port == port_of_node {
                connection.max_retries = max_retries;
                connection.delay_between_retries_in_seconds = delay_between_retries_in_seconds;
                break;
            }
        }
    }

    /// Dynamically allocates free ports for:
    /// - REST Server
    /// - Event Stream Server  
    ///
    /// Updates the ports in the config accordingly.
    pub(crate) fn allocate_available_ports(&mut self) {
        let rest_server_port = portpicker::pick_unused_port().expect("Error getting free port");
        let sse_server_port = portpicker::pick_unused_port().expect("Error getting free port");
        self.config.rest_server.port = rest_server_port;
        self.config.event_stream_server.port = sse_server_port;
    }

    /// Returns the inner [Config]
    pub(crate) fn inner(&self) -> Config {
        self.config.clone()
    }

    /// Returns the port that the sidecar is configured to connect to, i.e. the Fake Event Stream port.
    pub(crate) fn connection_ports(&self) -> Vec<u16> {
        self.config
            .connections
            .iter()
            .map(|conn| conn.sse_port)
            .collect::<Vec<u16>>()
    }

    /// Returns the port that the sidecar REST server is bound to.
    pub(crate) fn rest_server_port(&self) -> u16 {
        self.config.rest_server.port
    }

    /// Returns the port that the sidecar SSE server is bound to.
    pub(crate) fn event_stream_server_port(&self) -> u16 {
        self.config.event_stream_server.port
    }
}
