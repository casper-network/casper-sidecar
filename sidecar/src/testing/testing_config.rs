use casper_event_listener::FilterPriority;
use tempfile::TempDir;

use crate::types::config::Config;

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
    let path_to_temp_storage = temp_storage
        .path()
        .to_str()
        .expect("Error getting path of temporary directory")
        .to_string();

    TestingConfig::default()
        .set_storage_path(path_to_temp_storage)
        .allocate_available_ports()
}

impl TestingConfig {
    /// Creates a Default instance of TestingConfig which contains a Default instance of [Config]
    pub(crate) fn default() -> Self {
        let config = Config::default();

        Self { config }
    }

    /// Specify where test storage (database, sse cache) should be located.
    /// By default it is set to `/target/test_storage` however it is recommended to overwrite this with a `TempDir` path for testing purposes.
    pub(crate) fn set_storage_path(mut self, path: String) -> Self {
        self.config.storage.storage_path = path;
        self
    }

    /// Specify the port that the sidecar should connect to.
    /// By default it is set to `18101` - the SSE port of a node in the default NCTL network.
    pub(crate) fn set_connection_address(mut self, ip_address: Option<String>, port: u16) -> Self {
        if let Some(address) = ip_address {
            self.config.connections[0].ip_address = address;
        }
        self.config.connections[0].sse_port = port;
        self
    }

    /// Set how the sidecar should handle the case where it is only able to connect to one or two of the node's filters.
    pub(crate) fn set_allow_partial_connection(mut self, allow_partial_connection: bool) -> Self {
        self.config.connections[0].allow_partial_connection = allow_partial_connection;
        self
    }

    pub(crate) fn set_filter_priority(&mut self, filter_priority: FilterPriority) {
        self.config.connections[0].filter_priority = filter_priority;
    }

    /// Specify the retry configuration settings. By default they are set as follows:
    /// - `max_retries`: 3
    /// - `delay_between_retries_in_seconds`: 5
    pub(crate) fn configure_retry_settings(
        mut self,
        max_retries: u8,
        delay_between_retries_in_seconds: u8,
    ) -> Self {
        for connection in &mut self.config.connections {
            connection.max_retries = max_retries;
            connection.delay_between_retries_in_seconds = delay_between_retries_in_seconds;
        }
        self
    }

    /// Dynamically allocates free ports for:
    /// - Node Connection
    /// - REST Server
    /// - Event Stream Server  
    ///
    /// Updates the ports in the config accordingly.
    pub(crate) fn allocate_available_ports(mut self) -> Self {
        let node_connection_port = portpicker::pick_unused_port().expect("Error getting free port");
        let rest_server_port = portpicker::pick_unused_port().expect("Error getting free port");
        let sse_server_port = portpicker::pick_unused_port().expect("Error getting free port");
        self.config.rest_server.port = rest_server_port;
        self.config.event_stream_server.port = sse_server_port;
        self.config.connections[0].sse_port = node_connection_port;

        self
    }

    /// Returns the inner [Config]
    pub(crate) fn inner(&self) -> Config {
        self.config.clone()
    }

    /// Returns the port that the sidecar is configured to connect to, i.e. the Fake Event Stream port.
    pub(crate) fn connection_port(&self) -> u16 {
        self.config.connections[0].sse_port
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
