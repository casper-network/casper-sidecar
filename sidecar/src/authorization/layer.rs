use futures_util::future::BoxFuture;
use http::{header::AUTHORIZATION, StatusCode};
use hyper::{Body, Request, Response};
use std::sync::Arc;
use tower_http::auth::AsyncAuthorizeRequest;

use super::{Authorizer, UserId, BEARER};

#[derive(Clone)]
pub struct HeaderBasedAuthorizeRequest<T>
where
    T: Authorizer + Send + Sync + 'static,
{
    api_token_authorizer: Arc<T>,
}

impl<T> HeaderBasedAuthorizeRequest<T>
where
    T: Authorizer + Send + Sync + 'static,
{
    pub(crate) fn new(api_token_authorizer: T) -> Self {
        HeaderBasedAuthorizeRequest {
            api_token_authorizer: Arc::new(api_token_authorizer),
        }
    }
}

impl<B, T> AsyncAuthorizeRequest<B> for HeaderBasedAuthorizeRequest<T>
where
    B: Send + Sync + 'static,
    T: Authorizer + Send + Sync + 'static,
{
    type RequestBody = B;
    type ResponseBody = Body;
    type Future = BoxFuture<'static, Result<Request<B>, Response<Self::ResponseBody>>>;

    fn authorize(&mut self, mut request: Request<B>) -> Self::Future {
        let authorizer = self.api_token_authorizer.clone();
        Box::pin(async {
            if let Some(user_id) = check_auth(authorizer, &request).await {
                request.extensions_mut().insert(user_id);
                Ok(request)
            } else {
                Err(unauthorized_response())
            }
        })
    }
}

async fn check_auth<T: Authorizer, B>(
    api_token_authorizer: Arc<T>,
    request: &Request<B>,
) -> Option<UserId> {
    let maybe_token = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|header: &http::HeaderValue| header.to_str().ok());
    if let Some(token) = maybe_token {
        let parts = token.split(' ').collect::<Vec<&str>>();
        if parts.len() != 2 {
            return None;
        }
        if parts[0].to_lowercase() != BEARER {
            return None;
        }
        api_token_authorizer.authorize(parts[1].to_string()).await
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    struct MockAuthorizer {
        expected_token: Option<String>,
        user_id: Option<UserId>,
    }

    impl MockAuthorizer {
        fn new(expected_token: Option<String>, user_id: Option<UserId>) -> Self {
            MockAuthorizer {
                expected_token,
                user_id,
            }
        }
    }

    #[async_trait]
    impl Authorizer for MockAuthorizer {
        async fn authorize(&self, token: String) -> Option<UserId> {
            if let Some(expected_token) = self.expected_token.clone() {
                if expected_token != token {
                    unreachable!("Expected token {} but got {}", expected_token, token);
                }
            }
            self.user_id.clone()
        }
    }

    #[tokio::test]
    async fn should_fail_authorization_if_no_token_given() {
        let result = prepare_and_authorize(None, None, None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn should_fail_authorization_if_no_user_for_token() {
        let result = prepare_and_authorize(Some("xyz"), None, Some("Bearer xyz")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn should_fail_authorization_if_bearer_prefix_missing() {
        let result = prepare_and_authorize(Some("xyz"), Some("user_1"), Some("xyz")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn should_fail_authorization_if_correct_token_under_wrong_method() {
        let result = prepare_and_authorize(Some("abc"), Some("user_1"), Some("Basic abc")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn should_pass() {
        let result = prepare_and_authorize(Some("abc"), Some("user_1"), Some("Bearer abc")).await;
        let user_id = result
            .unwrap()
            .extensions()
            .get::<UserId>()
            .unwrap()
            .clone();
        assert_eq!(user_id, UserId("user_1".into()));
    }

    async fn prepare_and_authorize(
        expected_token: Option<&str>,
        user_id: Option<&str>,
        authorization_header: Option<&str>,
    ) -> Result<Request<Body>, Response<Body>> {
        let authorizer = MockAuthorizer::new(
            expected_token.map(|token| token.to_string()),
            user_id.map(|id| UserId(id.to_string())),
        );
        let mut auth = HeaderBasedAuthorizeRequest::new(authorizer);
        let mut builder = Request::builder();
        if let Some(header) = authorization_header {
            builder = builder.header("Authorization", header);
        }
        let request = builder.body(Body::empty()).unwrap();
        auth.authorize(request).await
    }
}

fn unauthorized_response() -> Response<Body> {
    Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .body(b"Unauthorized".to_vec().into())
        .unwrap()
}
