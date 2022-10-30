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
    pub delay_between_retries_in_seconds: u8,
    pub allow_partial_connection: bool,
    pub enable_logging: bool,
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
    pub max_concurrent_requests: u32,
    pub max_requests_per_second: u32,
    pub request_timeout_in_seconds: u32,
}

#[derive(Clone, Deserialize)]
pub struct EventStreamServerConfig {
    pub port: u16,
    pub max_concurrent_subscribers: u32,
    pub event_stream_buffer_length: u32,
}
