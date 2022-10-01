use serde::Deserialize;

#[derive(Clone, Deserialize)]
pub struct Config {
    pub node_connections: Vec<NodeConnection>,
    pub storage: StorageConfig,
    pub rest_server: ServerConfig,
    pub sse_server: ServerConfig,
}

#[derive(Clone, Deserialize)]
pub struct NodeConnection {
    pub ip_address: String,
    pub sse_port: u16,
    pub max_retries: u8,
    pub delay_between_retries_secs: u8,
}

#[derive(Clone, Deserialize)]
pub struct StorageConfig {
    pub storage_path: String,
    pub sse_cache_path: String,
    pub sqlite_config: SqliteConfig,
}

#[derive(Clone, Deserialize)]
pub struct SqliteConfig {
    pub file_name: String,
    pub max_write_connections: u32,
    pub max_read_connections: u32,
    pub wal_autocheckpointing_interval: u16,
}

#[derive(Clone, Deserialize)]
pub struct ServerConfig {
    pub ip_address: String,
    pub port: u16,
}
