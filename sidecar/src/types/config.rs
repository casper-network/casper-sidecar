use serde::Deserialize;

// This struct is used to parse the config.toml so the values can be utilised in the code.
#[derive(Clone, Deserialize)]
pub struct Config {
    pub connection: ConnectionConfig,
    pub storage: StorageConfig,
    pub rest_server: RestServerConfig,
    pub event_stream_server: EventStreamServerConfig,
}

#[derive(Clone, Deserialize)]
pub struct ConnectionConfig {
    pub node_connections: Vec<NodeConnection>,
}

#[derive(Clone, Deserialize)]
pub struct NodeConnection {
    pub ip_address: String,
    pub sse_port: u16,
    pub max_retries: u8,
    pub delay_between_retries_secs: u8,
    pub enable_event_logging: bool,
}

#[derive(Clone, Deserialize)]
pub struct StorageConfig {
    pub storage_path: String,
    pub sqlite_config: SqliteConfig,
}

#[derive(Clone, Deserialize)]
pub struct SqliteConfig {
    pub file_name: String,
    pub max_connections_in_pool: u32,
    pub wal_autocheckpointing_interval: u16,
}

#[derive(Clone, Deserialize)]
pub struct RestServerConfig {
    pub port: u16,
}

#[derive(Clone, Deserialize)]
pub struct EventStreamServerConfig {
    pub port: u16,
    pub max_concurrent_subscribers: u32,
    pub event_stream_buffer_length: u32,
}

#[cfg(test)]
impl Config {
    pub(crate) fn new() -> Self {
        Self {
            connection: ConnectionConfig {
                node_connections: vec![NodeConnection {
                    ip_address: "127.0.0.1".to_string(),
                    sse_port: 18101,
                    max_retries: 3,
                    delay_between_retries_secs: 3,
                    enable_event_logging: false,
                }],
            },
            storage: StorageConfig {
                storage_path: "/target/test_storage".to_string(),
                sqlite_config: SqliteConfig {
                    file_name: "test_sqlite_database".to_string(),
                    max_connections_in_pool: 100,
                    wal_autocheckpointing_interval: 1000,
                },
            },
            rest_server: RestServerConfig { port: 17777 },
            event_stream_server: EventStreamServerConfig {
                port: 19999,
                max_concurrent_subscribers: 100,
                event_stream_buffer_length: 5000,
            },
        }
    }
}
