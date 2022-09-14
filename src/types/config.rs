use serde::Deserialize;

#[derive(Deserialize)]
pub struct Config {
    pub node_connection: NodeConnection,
    pub storage: StorageConfig,
    pub rest_server: ServerConfig,
    pub sse_server: ServerConfig,
}

#[derive(Deserialize)]
pub struct NodeConnection {
    pub ip_address: String,
    pub sse_port: u16,
}

#[derive(Deserialize)]
pub struct StorageConfig {
    pub storage_path: String,
    pub sqlite_file_name: String,
    pub sse_cache_path: String,
}

#[derive(Deserialize)]
pub struct ServerConfig {
    pub ip_address: String,
    pub port: u16,
}
