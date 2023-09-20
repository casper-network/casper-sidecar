use thiserror::Error;

#[derive(Error, Debug)]
pub enum DatabaseConfigError {
    #[error("failed to parse the config option behind env_var {} with error: {}", .field_name, .error)]
    FieldNotFound {
        field_name: &'static str,
        error: String,
    },
    #[error("failed to parse the config option for {} with error: {}", .field_name, .error)]
    ParseError {
        field_name: &'static str,
        error: String,
    },
}
