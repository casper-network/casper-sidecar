use anyhow::{Context, Error};
use serde::Deserialize;

pub fn read_config(config_path: &str) -> Result<Config, Error> {
    let toml_content =
        std::fs::read_to_string(config_path).context("Error reading config file contents")?;
    toml::from_str(&toml_content).context("Error parsing config into TOML format")
}

// This struct is used to parse the toml-formatted config file so the values can be utilised in the code.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(Default))]
pub struct Config {
    pub inbound_channel_size: Option<usize>,
    pub outbound_channel_size: Option<usize>,
    pub connections: Vec<Connection>,
    pub storage: StorageConfig,
    pub rest_server: RestServerConfig,
    pub event_stream_server: EventStreamServerConfig,
    pub admin_server: Option<AdminServerConfig>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Connection {
    pub ip_address: String,
    pub sse_port: u16,
    pub rest_port: u16,
    pub max_attempts: usize,
    pub delay_between_retries_in_seconds: usize,
    pub allow_partial_connection: bool,
    pub enable_logging: bool,
    pub connection_timeout_in_seconds: Option<usize>,
    pub sleep_between_keep_alive_checks_in_seconds: Option<usize>,
    pub no_message_timeout_in_seconds: Option<usize>,
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

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct AdminServerConfig {
    pub port: u16,
    pub max_concurrent_requests: u32,
    pub max_requests_per_second: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_parse_nctl_config_toml() {
        let expected_config = Config {
            inbound_channel_size: None,
            outbound_channel_size: None,
            connections: vec![
                Connection {
                    ip_address: "127.0.0.1".to_string(),
                    sse_port: 18101,
                    rest_port: 14101,
                    max_attempts: 10,
                    delay_between_retries_in_seconds: 5,
                    allow_partial_connection: false,
                    enable_logging: true,
                    connection_timeout_in_seconds: None,
                    sleep_between_keep_alive_checks_in_seconds: None,
                    no_message_timeout_in_seconds: None,
                },
                Connection {
                    ip_address: "127.0.0.1".to_string(),
                    sse_port: 18102,
                    rest_port: 14102,
                    max_attempts: 10,
                    delay_between_retries_in_seconds: 5,
                    allow_partial_connection: false,
                    enable_logging: false,
                    connection_timeout_in_seconds: None,
                    sleep_between_keep_alive_checks_in_seconds: None,
                    no_message_timeout_in_seconds: None,
                },
                Connection {
                    ip_address: "127.0.0.1".to_string(),
                    sse_port: 18103,
                    rest_port: 14103,
                    max_attempts: 10,
                    delay_between_retries_in_seconds: 5,
                    allow_partial_connection: false,
                    enable_logging: false,
                    connection_timeout_in_seconds: Some(3),
                    sleep_between_keep_alive_checks_in_seconds: None,
                    no_message_timeout_in_seconds: None,
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
            admin_server: None,
        };

        let parsed_config = read_config("../EXAMPLE_NCTL_CONFIG.toml")
            .expect("Error parsing EXAMPLE_NCTL_CONFIG.toml");

        assert_eq!(parsed_config, expected_config);
    }

    #[test]
    fn should_parse_node_config_toml() {
        let expected_config = Config {
            inbound_channel_size: None,
            outbound_channel_size: None,
            connections: vec![Connection {
                ip_address: "127.0.0.1".to_string(),
                sse_port: 9999,
                rest_port: 8888,
                max_attempts: 10,
                delay_between_retries_in_seconds: 5,
                allow_partial_connection: false,
                enable_logging: true,
                connection_timeout_in_seconds: None,
                sleep_between_keep_alive_checks_in_seconds: None,
                no_message_timeout_in_seconds: None,
            }],
            storage: StorageConfig {
                storage_path: "/var/lib/casper-event-sidecar".to_string(),
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
            admin_server: Some(AdminServerConfig {
                port: 18887,
                max_concurrent_requests: 1,
                max_requests_per_second: 1,
            }),
        };

        let parsed_config = read_config("../EXAMPLE_NODE_CONFIG.toml")
            .expect("Error parsing EXAMPLE_NODE_CONFIG.toml");

        assert_eq!(parsed_config, expected_config);
    }

    impl Default for Connection {
        fn default() -> Self {
            Self {
                ip_address: "127.0.0.1".to_string(),
                sse_port: 18101,
                rest_port: 14101,
                allow_partial_connection: false,
                max_attempts: 3,
                delay_between_retries_in_seconds: 5,
                enable_logging: false,
                connection_timeout_in_seconds: None,
                sleep_between_keep_alive_checks_in_seconds: None,
                no_message_timeout_in_seconds: None,
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
