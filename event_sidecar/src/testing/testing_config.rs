#[cfg(test)]
use portpicker::Port;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

use crate::types::config::{Connection, SseEventServerConfig, StorageConfig};

/// A basic wrapper with helper methods for constructing and tweaking [Config]s for use in tests.
pub struct TestingConfig {
    pub(crate) config: SseEventServerConfig,
}

#[cfg(test)]
use once_cell::sync::Lazy;
#[cfg(test)]
static USED_PORTS: Lazy<Arc<Mutex<Vec<u16>>>> = Lazy::new(|| Arc::new(Mutex::new(Vec::new())));
#[cfg(test)]
/// This function (used in tests only) is used to make sure that concurrently running
/// IT tests don't accidentally pick the same port. If in the future our tests would run
/// slowly or not run at all because of this we need to figure out a way of returning ports after an IT test finishes
pub fn get_port() -> u16 {
    let mut guard = USED_PORTS.lock().unwrap();
    let mut maybe_port = portpicker::pick_unused_port().unwrap();
    let mut attempt = 0;
    while guard.contains(&maybe_port) {
        maybe_port = portpicker::pick_unused_port().unwrap();
        attempt += 1;
        if attempt > 100 {
            panic!("Couldn't find a unique port in {} tries!", attempt);
        }
    }
    guard.push(maybe_port);
    maybe_port
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
        let config = SseEventServerConfig::default();

        Self { config }
    }

    /// Specify where test storage (database, sse cache) should be located.
    /// By default it is set to `/target/test_storage` however it is recommended to overwrite this with a `TempDir` path for testing purposes.
    pub(crate) fn set_storage_path(&mut self, path: String) {
        self.config.storage.set_storage_path(path);
    }

    pub(crate) fn set_storage(&mut self, storage: StorageConfig) {
        self.config.storage = storage;
    }

    pub(crate) fn add_connection(
        &mut self,
        ip_address: Option<String>,
        sse_port: Option<u16>,
        rest_port: Option<u16>,
    ) -> Port {
        let random_port_for_sse = get_port();
        let random_port_for_rest = get_port();
        let connection = Connection {
            ip_address: ip_address.unwrap_or_else(|| "127.0.0.1".to_string()),
            sse_port: sse_port.unwrap_or(random_port_for_sse),
            rest_port: rest_port.unwrap_or(random_port_for_rest),
            max_attempts: 2,
            delay_between_retries_in_seconds: 0,
            allow_partial_connection: false,
            enable_logging: false,
            connection_timeout_in_seconds: Some(100),
            sleep_between_keep_alive_checks_in_seconds: Some(100),
            no_message_timeout_in_seconds: Some(100),
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
        for connection in &mut self.config.connections {
            if connection.sse_port == port_of_node {
                connection.allow_partial_connection = allow_partial_connection;
                break;
            }
        }
    }

    /// Specify the retry configuration settings. By default they are set as follows:
    /// - `max_attempts`: 3
    /// - `delay_between_retries_in_seconds`: 5
    pub(crate) fn set_retries_for_node(
        &mut self,
        port_of_node: u16,
        max_attempts: usize,
        delay_between_retries_in_seconds: usize,
    ) {
        for connection in &mut self.config.connections {
            if connection.sse_port == port_of_node {
                connection.max_attempts = max_attempts;
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
        let rest_server_port = get_port();
        let sse_server_port = get_port();
        self.config.rest_server.port = rest_server_port;
        self.config.event_stream_server.port = sse_server_port;
    }

    /// Returns the inner [Config]
    pub(crate) fn inner(&self) -> SseEventServerConfig {
        self.config.clone()
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
