use serde::Deserialize;
use std::{
    convert::{TryFrom, TryInto},
    net::IpAddr,
    num::ParseIntError,
    string::ToString,
};

use crate::database::{
    database_errors::DatabaseConfigError,
    env_vars::{
        get_connection_information_from_env, DATABASE_HOST_ENV_VAR_KEY,
        DATABASE_MAX_CONNECTIONS_ENV_VAR_KEY, DATABASE_NAME_ENV_VAR_KEY,
        DATABASE_PASSWORD_ENV_VAR_KEY, DATABASE_PORT_ENV_VAR_KEY, DATABASE_USERNAME_ENV_VAR_KEY,
    },
};

/// The default postgres max connections.
pub(crate) const DEFAULT_MAX_CONNECTIONS: u32 = 10;
/// The default postgres port.
pub(crate) const DEFAULT_PORT: u16 = 5432;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub enum LegacySseApiTag {
    // This tag is to point to sse endpoint of casper node in version 1.x
    V1,
}
fn default_disable_event_persistence() -> bool {
    false
}
// This struct is used to parse the toml-formatted config file so the values can be utilised in the code.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct SseEventServerConfig {
    pub enable_server: bool,
    pub emulate_legacy_sse_apis: Option<Vec<LegacySseApiTag>>,
    pub inbound_channel_size: Option<usize>,
    pub outbound_channel_size: Option<usize>,
    pub connections: Vec<Connection>,
    pub event_stream_server: EventStreamServerConfig,
    #[serde(default = "default_disable_event_persistence")]
    pub disable_event_persistence: bool,
}

#[cfg(any(feature = "testing", test))]
impl Default for SseEventServerConfig {
    fn default() -> Self {
        Self {
            enable_server: true,
            emulate_legacy_sse_apis: Some(vec![LegacySseApiTag::V1]),
            inbound_channel_size: Some(100),
            outbound_channel_size: Some(100),
            connections: Vec::new(),
            event_stream_server: EventStreamServerConfig::default(),
            disable_event_persistence: false,
        }
    }
}

impl SseEventServerConfig {
    #[cfg(any(feature = "testing", test))]
    pub fn default_no_persistence() -> Self {
        Self {
            disable_event_persistence: true,
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Connection {
    pub ip_address: IpAddr,
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

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub struct StorageConfig {
    pub storage_folder: String,
    pub sqlite_config: Option<SqliteConfig>,
    pub postgresql_config: Option<PostgresqlConfig>,
}

impl StorageConfig {
    pub fn is_enabled(&self) -> bool {
        self.sqlite_config
            .as_ref()
            .map(|config| config.enabled)
            .unwrap_or_else(|| {
                self.postgresql_config
                    .as_ref()
                    .map(|config| config.enabled)
                    .unwrap_or(false)
            })
    }

    #[cfg(test)]
    pub(crate) fn set_storage_folder(&mut self, path: String) {
        self.storage_folder = path.clone();
    }

    #[cfg(test)]
    pub(crate) fn clear_db_storage(&mut self) {
        self.sqlite_config = None;
        self.postgresql_config = None;
    }

    #[cfg(any(feature = "testing", test))]
    pub fn two_dbs() -> Self {
        StorageConfig {
            storage_folder: "abc".to_string(),
            sqlite_config: Some(SqliteConfig::default()),
            postgresql_config: Some(PostgresqlConfig::default()),
        }
    }

    #[cfg(test)]
    pub fn postgres_with_port(port: u16) -> Self {
        Self {
            storage_folder: "storage".to_string(),
            sqlite_config: None,
            postgresql_config: Some(PostgresqlConfig {
                enabled: true,
                host: "localhost".to_string(),
                database_name: "event_sidecar".to_string(),
                database_username: "postgres".to_string(),
                database_password: "p@$$w0rd".to_string(),
                max_connections_in_pool: 100,
                port,
            }),
        }
    }

    #[cfg(any(feature = "testing", test))]
    pub fn no_dbs() -> Self {
        Self {
            storage_folder: "storage".to_string(),
            sqlite_config: None,
            postgresql_config: None,
        }
    }

    #[cfg(any(feature = "testing", test))]
    pub fn no_enabled_dbs() -> Self {
        let sqlite_config = SqliteConfig {
            enabled: false,
            ..Default::default()
        };
        let postgresql_config = PostgresqlConfig {
            enabled: false,
            ..Default::default()
        };
        Self {
            storage_folder: "storage".to_string(),
            sqlite_config: Some(sqlite_config),
            postgresql_config: Some(postgresql_config),
        }
    }

    pub fn is_postgres_enabled(&self) -> bool {
        self.postgresql_config
            .as_ref()
            .map(|config| config.enabled)
            .unwrap_or(false)
    }

    pub fn is_sqlite_enabled(&self) -> bool {
        self.sqlite_config
            .as_ref()
            .map(|config| config.enabled)
            .unwrap_or(false)
    }
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub struct StorageConfigSerdeTarget {
    storage_folder: String,
    sqlite_config: Option<SqliteConfig>,
    postgresql_config: Option<PostgresqlConfigSerdeTarget>,
}

impl Default for StorageConfigSerdeTarget {
    fn default() -> Self {
        Self {
            storage_folder: "storage".to_string(),
            sqlite_config: None,
            postgresql_config: Some(PostgresqlConfigSerdeTarget::default()),
        }
    }
}
impl TryFrom<StorageConfigSerdeTarget> for StorageConfig {
    type Error = DatabaseConfigError;

    fn try_from(value: StorageConfigSerdeTarget) -> Result<Self, Self::Error> {
        let mut storage_cofnig = Self {
            storage_folder: value.storage_folder,
            ..Default::default()
        };
        if let Some(config) = value.sqlite_config {
            storage_cofnig.sqlite_config = Some(config);
        } else if let Some(config) = value.postgresql_config {
            let postgresql_config: PostgresqlConfig = config.try_into()?;
            storage_cofnig.postgresql_config = Some(postgresql_config);
        };
        Ok(storage_cofnig)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct SqliteConfig {
    pub enabled: bool,
    pub file_name: String,
    pub max_connections_in_pool: u32,
    pub wal_autocheckpointing_interval: u16,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct PostgresqlConfig {
    pub enabled: bool,
    pub host: String,
    pub database_name: String,
    pub database_username: String,
    pub database_password: String,
    pub max_connections_in_pool: u32,
    pub port: u16,
}

#[cfg(any(feature = "testing", test))]
impl Default for PostgresqlConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            host: "localhost".to_string(),
            database_name: "event_sidecar".to_string(),
            database_username: "postgres".to_string(),
            database_password: "p@$$w0rd".to_string(),
            max_connections_in_pool: 100,
            port: 5432,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
pub struct PostgresqlConfigSerdeTarget {
    pub enabled: bool,
    pub host: Option<String>,
    pub database_name: Option<String>,
    pub database_username: Option<String>,
    pub database_password: Option<String>,
    pub max_connections_in_pool: Option<u32>,
    pub port: Option<u16>,
}

impl TryFrom<PostgresqlConfigSerdeTarget> for PostgresqlConfig {
    type Error = DatabaseConfigError;

    fn try_from(value: PostgresqlConfigSerdeTarget) -> Result<Self, Self::Error> {
        let host = get_connection_information_from_env(DATABASE_HOST_ENV_VAR_KEY, value.host)?;

        let database_name =
            get_connection_information_from_env(DATABASE_NAME_ENV_VAR_KEY, value.database_name)?;

        let database_username = get_connection_information_from_env(
            DATABASE_USERNAME_ENV_VAR_KEY,
            value.database_username,
        )?;

        let database_password = get_connection_information_from_env(
            DATABASE_PASSWORD_ENV_VAR_KEY,
            value.database_password,
        )?;

        let max_connections: u32 = get_connection_information_from_env(
            DATABASE_MAX_CONNECTIONS_ENV_VAR_KEY,
            value.max_connections_in_pool,
        )
        .unwrap_or(DEFAULT_MAX_CONNECTIONS.to_string())
        .parse()
        .map_err(|e: ParseIntError| DatabaseConfigError::ParseError {
            field_name: "Max connections",
            error: e.to_string(),
        })?;

        let port: u16 = get_connection_information_from_env(DATABASE_PORT_ENV_VAR_KEY, value.port)
            .unwrap_or(DEFAULT_PORT.to_string())
            .parse()
            .map_err(|e: ParseIntError| DatabaseConfigError::ParseError {
                field_name: "Port",
                error: e.to_string(),
            })?;

        Ok(PostgresqlConfig {
            enabled: value.enabled,
            host,
            database_name,
            database_username,
            database_password,
            max_connections_in_pool: max_connections,
            port,
        })
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct RestApiServerConfig {
    pub enable_server: bool,
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
pub struct AdminApiServerConfig {
    pub enable_server: bool,
    pub port: u16,
    pub max_concurrent_requests: u32,
    pub max_requests_per_second: u32,
}

#[cfg(any(feature = "testing", test))]
impl Default for AdminApiServerConfig {
    fn default() -> Self {
        Self {
            enable_server: true,
            port: 1211,
            max_concurrent_requests: 50,
            max_requests_per_second: 60,
        }
    }
}

#[cfg(any(feature = "testing", test))]
mod tests {
    use std::net::Ipv4Addr;

    use super::*;

    impl Connection {
        pub fn example_connection_1() -> Connection {
            Connection {
                ip_address: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                sse_port: 18101,
                rest_port: 14101,
                max_attempts: 10,
                delay_between_retries_in_seconds: 5,
                allow_partial_connection: false,
                enable_logging: true,
                connection_timeout_in_seconds: None,
                sleep_between_keep_alive_checks_in_seconds: None,
                no_message_timeout_in_seconds: None,
            }
        }

        pub fn example_connection_2() -> Connection {
            Connection {
                ip_address: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                sse_port: 18102,
                rest_port: 14102,
                max_attempts: 10,
                delay_between_retries_in_seconds: 5,
                allow_partial_connection: false,
                enable_logging: false,
                connection_timeout_in_seconds: None,
                sleep_between_keep_alive_checks_in_seconds: None,
                no_message_timeout_in_seconds: None,
            }
        }

        pub fn example_connection_3() -> Connection {
            Connection {
                ip_address: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                sse_port: 18103,
                rest_port: 14103,
                max_attempts: 10,
                delay_between_retries_in_seconds: 5,
                allow_partial_connection: false,
                enable_logging: false,
                connection_timeout_in_seconds: Some(3),
                sleep_between_keep_alive_checks_in_seconds: None,
                no_message_timeout_in_seconds: None,
            }
        }
    }

    impl Default for Connection {
        fn default() -> Self {
            Self {
                ip_address: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
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
            StorageConfig {
                storage_folder: "abc".to_string(),
                sqlite_config: Some(SqliteConfig::default()),
                postgresql_config: None,
            }
        }
    }

    impl Default for SqliteConfig {
        fn default() -> Self {
            Self {
                enabled: true,
                file_name: "test_sqlite_database".to_string(),
                max_connections_in_pool: 100,
                wal_autocheckpointing_interval: 1000,
            }
        }
    }

    impl Default for RestApiServerConfig {
        fn default() -> Self {
            Self {
                enable_server: true,
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
