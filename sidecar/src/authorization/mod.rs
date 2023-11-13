use crate::types::config::AuthConfig;
use async_trait::async_trait;
pub use layer::HeaderBasedAuthorizeRequest;
use std::collections::HashMap;
mod layer;

const BEARER: &str = "bearer";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UserId(String);

#[async_trait]
pub trait Authorizer {
    async fn authorize(&self, token: String) -> Option<UserId>;
}

#[derive(Clone)]
pub struct BearerAuthorizer {
    api_key_to_username: HashMap<String, String>,
}

#[async_trait]
impl Authorizer for BearerAuthorizer {
    async fn authorize(&self, token: String) -> Option<UserId> {
        self.api_key_to_username
            .get(&token)
            .map(|username| UserId(username.clone()))
    }
}

impl BearerAuthorizer {
    pub fn new(token_user_map: HashMap<String, String>) -> Self {
        BearerAuthorizer {
            api_key_to_username: token_user_map,
        }
    }
}

pub enum AuthorizationValidator {
    SimpleAuthorization(BearerAuthorizer),
    NoAuthorization,
}

pub fn build_authorization_validator(auth_config: &Option<AuthConfig>) -> AuthorizationValidator {
    if let Some(auth) = auth_config {
        if auth.enabled {
            let token_to_user = auth
                .users
                .iter()
                .map(|user| (user.api_key.clone(), user.username.clone()))
                .collect();
            AuthorizationValidator::SimpleAuthorization(BearerAuthorizer::new(token_to_user))
        } else {
            AuthorizationValidator::NoAuthorization
        }
    } else {
        AuthorizationValidator::NoAuthorization
    }
}
