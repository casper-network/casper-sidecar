use serde::Deserialize;

// This struct is used to parse the EXAMPLE_CONFIG.toml so the values can be utilised in the code.
#[derive(Clone, Deserialize)]
pub struct Config {
    pub connection: ConnectionConfig,
    pub storage: StorageConfig,
    pub rest_server: RestServerConfig,
    pub event_stream_server: EventStreamServerConfig,
}

#[cfg(test)]
impl Default for Config {
    fn default() -> Self {
        Self {
            connection: ConnectionConfig::default(),
            storage: StorageConfig::default(),
            rest_server: RestServerConfig::default(),
            event_stream_server: EventStreamServerConfig::default(),
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct ConnectionConfig {
    pub node_connections: Vec<NodeConnection>,
}

#[cfg(test)]
impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            node_connections: vec![NodeConnection::default()],
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct NodeConnection {
    pub ip_address: String,
    pub sse_port: u16,
    pub max_retries: u8,
    pub delay_between_retries_in_seconds: u8,
    pub allow_partial_connection: bool,
    pub enable_logging: bool,
}

#[cfg(test)]
impl Default for NodeConnection {
    fn default() -> Self {
        Self {
            ip_address: "127.0.0.1".to_string(),
            sse_port: 18101,
            allow_partial_connection: false,
            max_retries: 3,
            delay_between_retries_in_seconds: 5,
            enable_logging: false,
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct StorageConfig {
    pub storage_path: String,
    pub sqlite_config: SqliteConfig,
}

#[cfg(test)]
impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            storage_path: "/target/test_storage".to_string(),
            sqlite_config: SqliteConfig::default(),
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct SqliteConfig {
    pub file_name: String,
    pub max_connections_in_pool: u32,
    pub wal_autocheckpointing_interval: u16,
}

#[cfg(test)]
impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            file_name: "test_sqlite_database".to_string(),
            max_connections_in_pool: 100,
            wal_autocheckpointing_interval: 1000,
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct RestServerConfig {
    pub port: u16,
    pub max_concurrent_requests: u32,
    pub max_requests_per_second: u32,
    pub request_timeout_in_seconds: u32,
}

#[cfg(test)]
impl Default for RestServerConfig {
    fn default() -> Self {
        Self {
            port: 17777,
            max_concurrent_requests: 50,
            max_requests_per_second: 50,
            request_timeout_in_seconds: 20,
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct EventStreamServerConfig {
    pub port: u16,
    pub max_concurrent_subscribers: u32,
    pub event_stream_buffer_length: u32,
}

#[cfg(test)]
impl Default for EventStreamServerConfig {
    fn default() -> Self {
        Self {
            port: 19999,
            max_concurrent_subscribers: 100,
            event_stream_buffer_length: 5000,
        }
    }
}
