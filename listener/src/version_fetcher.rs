use anyhow::{anyhow, Context, Error};
use async_trait::async_trait;
use casper_types::ProtocolVersion;
use once_cell::sync::Lazy;
use serde_json::Value;
use std::str::FromStr;
use tracing::debug;
use url::Url;

const BUILD_VERSION_KEY: &str = "build_version";

static MINIMAL_NODE_VERSION: Lazy<ProtocolVersion> =
    Lazy::new(|| ProtocolVersion::from_parts(1, 5, 2));

#[derive(Debug)]
pub enum BuildVersionFetchError {
    Error(anyhow::Error),
    VersionNotAcceptable(String),
}

#[cfg(test)]
impl Clone for BuildVersionFetchError {
    fn clone(&self) -> Self {
        match self {
            Self::Error(err) => Self::Error(Error::msg(err.to_string())),
            Self::VersionNotAcceptable(arg0) => Self::VersionNotAcceptable(arg0.clone()),
        }
    }
}

#[async_trait]
pub trait VersionFetcher: Sync + Send {
    async fn fetch(&self) -> Result<ProtocolVersion, BuildVersionFetchError>;
}
pub fn for_status_endpoint(status_endpoint: Url) -> impl VersionFetcher {
    StatusEndpointVersionFetcher { status_endpoint }
}

#[derive(Clone)]
pub struct StatusEndpointVersionFetcher {
    status_endpoint: Url,
}

#[async_trait]
impl VersionFetcher for StatusEndpointVersionFetcher {
    async fn fetch(&self) -> Result<ProtocolVersion, BuildVersionFetchError> {
        let status_endpoint = self.status_endpoint.clone();
        debug!("Fetching build version for {}", status_endpoint);
        match fetch_build_version_from_status(status_endpoint).await {
            Ok(version) => {
                validate_version(&version)?;
                Ok(version)
            }
            Err(fetch_err) => Err(BuildVersionFetchError::Error(fetch_err)),
        }
    }
}

// Fetch the build version by requesting the status from the node's rest server.
async fn fetch_build_version_from_status(status_endpoint: Url) -> Result<ProtocolVersion, Error> {
    let status_response = reqwest::get(status_endpoint)
        .await
        .context("Should have responded with status")?;

    // The exact structure of the response varies over the versions but there is an assertion made
    // that .build_version is always valid. So the key is accessed without deserialising the full response.
    let response_json: Value = status_response
        .json()
        .await
        .context("Should have parsed JSON from response")?;

    try_resolve_version(response_json)
}

fn validate_version(version: &ProtocolVersion) -> Result<(), BuildVersionFetchError> {
    if version.lt(&MINIMAL_NODE_VERSION) {
        let msg = format!(
            "Node version expected to be >= {}.",
            MINIMAL_NODE_VERSION.value(),
        );
        Err(BuildVersionFetchError::VersionNotAcceptable(msg))
    } else {
        Ok(())
    }
}

fn try_resolve_version(raw_response: Value) -> Result<ProtocolVersion, Error> {
    match raw_response.get(BUILD_VERSION_KEY) {
        Some(build_version_value) if build_version_value.is_string() => {
            let raw = build_version_value
                .as_str()
                .context("build_version_value should be a string")
                .map_err(|e| {
                    count_error("version_value_not_a_string");
                    e
                })?
                .split('-')
                .next()
                .context("splitting build_version_value should always return at least one slice")
                .map_err(|e| {
                    count_error("incomprehensible_build_version_form");
                    e
                })?;
            ProtocolVersion::from_str(raw).map_err(|error| {
                count_error("failed_parsing_protocol_version");
                anyhow!("failed parsing build version from '{}': {}", raw, error)
            })
        }
        _ => {
            count_error("failed_getting_status_from_payload");
            Err(anyhow!(
                "failed to get {} from status response {}",
                BUILD_VERSION_KEY,
                raw_response
            ))
        }
    }
}

fn count_error(reason: &str) {
    casper_event_types::metrics::ERROR_COUNTS
        .with_label_values(&["fetching_build_version_for_node", reason])
        .inc();
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use casper_types::{ProtocolVersion, SemVer};
    use mockito::{Mock, Server, ServerGuard};
    use serde_json::json;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn try_resolve_version_should_interpret_cortest_by_build_versionrect_build_version() {
        let mut protocol = test_by_build_version(Some("5.1.111-b94c4f79a"))
            .await
            .unwrap();
        assert_eq!(protocol, ProtocolVersion::new(SemVer::new(5, 1, 111)));

        protocol = test_by_build_version(Some("6.2.112-b94c4f79a-casper-mainnet"))
            .await
            .unwrap();
        assert_eq!(protocol, ProtocolVersion::new(SemVer::new(6, 2, 112)));

        protocol = test_by_build_version(Some("7.3.113")).await.unwrap();
        assert_eq!(protocol, ProtocolVersion::new(SemVer::new(7, 3, 113)));

        let version_validation_failed = test_by_build_version(Some("1.5.1")).await;
        assert!(matches!(
            version_validation_failed,
            Err(BuildVersionFetchError::VersionNotAcceptable(_))
        ));
    }

    #[tokio::test]
    async fn try_resolve_should_fail_if_build_version_is_absent() {
        let ret = test_by_build_version(None).await;
        assert!(ret.is_err());
    }

    #[tokio::test]
    async fn try_resolve_should_fail_if_build_version_is_invalid() {
        let ret = test_by_build_version(Some("not-a-semver")).await;
        assert!(ret.is_err());
    }

    fn build_server_mock(build_version: Option<&str>) -> (Mock, String, ServerGuard) {
        let mut server = Server::new();
        let url = format!("{}/status", server.url());
        let json_object = match build_version {
            Some(version) => json!({ BUILD_VERSION_KEY: version }),
            None => json!({}),
        };
        let raw_json = json_object.to_string();
        let mock = server
            .mock("GET", "/status")
            .with_status(201)
            .with_header("content-type", "application/json")
            .with_body(raw_json)
            .create();
        //We need to return the server guard here otherwise it will get dropped and the mock won't work correctly.
        (mock, url, server)
    }

    async fn test_by_build_version(
        build_version: Option<&str>,
    ) -> Result<ProtocolVersion, BuildVersionFetchError> {
        let (mock, url, _server) = build_server_mock(build_version);
        let result = for_status_endpoint(Url::parse(&url).unwrap()).fetch().await;
        mock.assert();
        result
    }

    pub struct MockVersionFetcher {
        repeatable: bool,
        version_responses: Mutex<Vec<Result<ProtocolVersion, BuildVersionFetchError>>>,
    }

    impl MockVersionFetcher {
        pub fn repeatable_from_protocol_version(version: &str) -> Self {
            let protocol_version = ProtocolVersion::from_str(version).unwrap();
            Self {
                repeatable: true,
                version_responses: Mutex::new(vec![Ok(protocol_version)]),
            }
        }
        pub fn new(
            version_responses: Vec<Result<ProtocolVersion, BuildVersionFetchError>>,
        ) -> Self {
            Self {
                repeatable: false,
                version_responses: Mutex::new(version_responses),
            }
        }
    }

    #[async_trait]
    impl VersionFetcher for MockVersionFetcher {
        async fn fetch(&self) -> Result<ProtocolVersion, BuildVersionFetchError> {
            let mut version_responses = self.version_responses.lock().await;
            if self.repeatable {
                return version_responses[0].clone();
            }
            version_responses.pop().unwrap() //If we are fetching something that wasn't prepared it should be an error
        }
    }
}
