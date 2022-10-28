use crate::types::config::Config;
use std::fs;
use std::path::Path;
use tempfile::TempDir;

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
    pub(crate) fn set_connection_port(mut self, port: u16) -> Self {
        self.config.connection.node_connections[0].sse_port = port;
        self
    }

    /// Specify the max_concurrent_subscribers for the outbound EventStreamServer. By default it is set to 100.
    pub(crate) fn set_max_sse_subscribers(mut self, num_subscribers: u32) -> Self {
        self.config.event_stream_server.max_concurrent_subscribers = num_subscribers;
        self
    }

    /// Specify the retry configuration settings. By default they are set as follows:
    /// - `max_retries`: 3
    /// - `delay_between_retries_in_seconds`: 5
    pub(crate) fn configure_retry_settings(
        mut self,
        max_retries: u8,
        delay_between_retries_in_seconds: u8,
    ) -> Self {
        for connection in &mut self.config.connection.node_connections {
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
        self.config.connection.node_connections[0].sse_port = node_connection_port;

        self
    }

    /// Returns the inner [Config]
    pub(crate) fn inner(&self) -> Config {
        self.config.clone()
    }

    /// Returns the port that the sidecar is configured to connect to, i.e. the Fake Event Stream port.
    pub(crate) fn connection_port(&self) -> u16 {
        self.config.connection.node_connections[0].sse_port
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

// Despite using TempDir I found that sometimes the directory wasn't being removed at the end of the tests.
// To that end I've added a manual check and removal here.
impl Drop for TestingConfig {
    fn drop(&mut self) {
        let config = self.inner();
        let path_to_temp_dir = Path::new(&config.storage.storage_path);
        if path_to_temp_dir.exists() {
            let _ = fs::remove_dir_all(path_to_temp_dir).map_err(|fs_err| {
                println!("Failed to remove temporary directory during Drop of TestingConfig")
            });
        }
    }
}
