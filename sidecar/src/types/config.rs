use anyhow::{Context, Error};
use serde::Deserialize;

pub fn read_config(config_path: &str) -> Result<Config, Error> {
    let toml_content =
        std::fs::read_to_string(config_path).context("Error reading config file contents")?;
    toml::from_str(&toml_content).context("Error parsing config into TOML format")
}

// This struct is used to parse the EXAMPLE_CONFIG.toml so the values can be utilised in the code.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(Default))]
pub struct Config {
    pub inbound_channel_size: Option<usize>,
    pub outbound_channel_size: Option<usize>,
    pub connections: Vec<Connection>,
    pub storage: StorageConfig,
    pub rest_server: RestServerConfig,
    pub event_stream_server: EventStreamServerConfig,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Connection {
    pub ip_address: String,
    pub sse_port: u16,
    pub rest_port: u16,
    pub max_retries: usize,
    pub delay_between_retries_in_seconds: usize,
    pub allow_partial_connection: bool,
    pub enable_logging: bool,
    pub connection_timeout_in_seconds: Option<usize>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct StorageConfig {
    pub storage_path: String,
    pub sqlite_config: SqliteConfig,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct SqliteConfig {
    pub file_name: String,
    pub max_connections_in_pool: u32,
    pub wal_autocheckpointing_interval: u16,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct RestServerConfig {
    pub port: u16,
    pub max_concurrent_requests: u32,
    pub max_requests_per_second: u32,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct EventStreamServerConfig {
    pub port: u16,
    pub max_concurrent_subscribers: u32,
    pub event_stream_buffer_length: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_parse_config_toml() {
        let example_config = Config {
            inbound_channel_size: None,
            outbound_channel_size: None,
            connections: vec![
                Connection {
                    ip_address: "127.0.0.1".to_string(),
                    sse_port: 18101,
                    rest_port: 14101,
                    max_retries: 10,
                    delay_between_retries_in_seconds: 5,
                    allow_partial_connection: false,
                    enable_logging: true,
                    connection_timeout_in_seconds: None,
                },
                Connection {
                    ip_address: "127.0.0.1".to_string(),
                    sse_port: 18102,
                    rest_port: 14102,
                    max_retries: 10,
                    delay_between_retries_in_seconds: 5,
                    allow_partial_connection: false,
                    enable_logging: false,
                    connection_timeout_in_seconds: None,
                },
                Connection {
                    ip_address: "127.0.0.1".to_string(),
                    sse_port: 18103,
                    rest_port: 14103,
                    max_retries: 10,
                    delay_between_retries_in_seconds: 5,
                    allow_partial_connection: false,
                    enable_logging: false,
                    connection_timeout_in_seconds: Some(3),
                },
            ],
            storage: StorageConfig {
                storage_path: "./target/storage".to_string(),
                sqlite_config: SqliteConfig {
                    file_name: "sqlite_database.db3".to_string(),
                    max_connections_in_pool: 100,
                    wal_autocheckpointing_interval: 1000,
                },
            },
            rest_server: RestServerConfig {
                port: 18888,
                max_concurrent_requests: 50,
                max_requests_per_second: 50,
            },
            event_stream_server: EventStreamServerConfig {
                port: 19999,
                max_concurrent_subscribers: 100,
                event_stream_buffer_length: 5000,
            },
        };

        let parsed_config =
            read_config("../EXAMPLE_CONFIG.toml").expect("Error parsing EXAMPLE_CONFIG.toml");

        assert_eq!(parsed_config, example_config);
    }

    impl Default for Connection {
        fn default() -> Self {
            Self {
                ip_address: "127.0.0.1".to_string(),
                sse_port: 18101,
                rest_port: 14101,
                allow_partial_connection: false,
                max_retries: 3,
                delay_between_retries_in_seconds: 5,
                enable_logging: false,
                connection_timeout_in_seconds: None,
            }
        }
    }

    impl Default for StorageConfig {
        fn default() -> Self {
            Self {
                storage_path: "/target/test_storage".to_string(),
                sqlite_config: SqliteConfig::default(),
            }
        }
    }

    impl Default for SqliteConfig {
        fn default() -> Self {
            Self {
                file_name: "test_sqlite_database".to_string(),
                max_connections_in_pool: 100,
                wal_autocheckpointing_interval: 1000,
            }
        }
    }

    impl Default for RestServerConfig {
        fn default() -> Self {
            Self {
                port: 17777,
                max_concurrent_requests: 50,
                max_requests_per_second: 50,
            }
        }
    }

    impl Default for EventStreamServerConfig {
        fn default() -> Self {
            Self {
                port: 19999,
                max_concurrent_subscribers: 100,
                event_stream_buffer_length: 5000,
            }
        }
    }
}
