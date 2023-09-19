use std::env;

use crate::database::database_errors::DatabaseConfigError;

/// The environment variable key for the Postgres username.
pub(crate) const DATABASE_USERNAME_ENV_VAR_KEY: &str = "SIDECAR_POSTGRES_USERNAME";
/// The environment variable key for the Postgres password.
pub(crate) const DATABASE_PASSWORD_ENV_VAR_KEY: &str = "SIDECAR_POSTGRES_PASSWORD";
/// The environment variable key for the Postgres database name.
pub(crate) const DATABASE_NAME_ENV_VAR_KEY: &str = "SIDECAR_POSTGRES_DATABASE_NAME";
/// The environment variable key for the Postgres host.
pub(crate) const DATABASE_HOST_ENV_VAR_KEY: &str = "SIDECAR_POSTGRES_HOST";
/// The environment variable key for the Postgres max connections.
pub(crate) const DATABASE_MAX_CONNECTIONS_ENV_VAR_KEY: &str = "SIDECAR_POSTGRES_MAX_CONNECTIONS";
/// The environment variable key for the Postgres port.
pub(crate) const DATABASE_PORT_ENV_VAR_KEY: &str = "SIDECAR_POSTGRES_PORT";

/// This function will return the value of the environment variable with the key `key`.
///
/// If the environment variable is not set, the function will check if a value has been set in the config file
/// and return that value.
///
/// If no value has been set in the config file, the function will return a `StorageConfigError` indicating that the
/// configuration for the database was not valid.
///
/// Note: The empty string will likely cause an error in setting up the db. This is expected and should
/// happen if the config value is not set in either place.
pub(crate) fn get_connection_information_from_env<T: ToString>(
    key: &'static str,
    config_backup: Option<T>,
) -> Result<String, DatabaseConfigError> {
    match env::var(key) {
        Ok(value) => Ok(value),
        Err(_) => match config_backup {
            Some(config_value) => Ok(config_value.to_string()),
            None => Err(DatabaseConfigError::FieldNotFound {
                field_name: key,
                error: "Config value not set in ENV vars or config file".to_string(),
            }),
        },
    }
}
