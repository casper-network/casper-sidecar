use anyhow::{anyhow, Context, Error};
use async_trait::async_trait;
use casper_types::ProtocolVersion;
use once_cell::sync::Lazy;
use serde_json::Value;
use std::str::FromStr;
use tracing::debug;
use url::Url;

const BUILD_VERSION_KEY: &str = "build_version";
const CHAINSPEC_NAME_KEY: &str = "chainspec_name";

static MINIMAL_NODE_VERSION: Lazy<ProtocolVersion> =
    Lazy::new(|| ProtocolVersion::from_parts(2, 0, 0));

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

#[derive(Eq, PartialEq, Clone, Default, Debug)]
pub struct NodeMetadata {
    pub build_version: ProtocolVersion,
    pub network_name: String,
}

impl NodeMetadata {
    pub fn validate(&self) -> Result<(), BuildVersionFetchError> {
        if self.build_version.lt(&MINIMAL_NODE_VERSION) {
            let msg = format!(
                "Node version expected to be >= {}.",
                MINIMAL_NODE_VERSION.value(),
            );
            Err(BuildVersionFetchError::VersionNotAcceptable(msg))
        } else {
            Ok(())
        }
    }
}

#[async_trait]
pub trait NodeMetadataFetcher: Sync + Send {
    async fn fetch(&self) -> Result<NodeMetadata, BuildVersionFetchError>;
}

pub fn for_status_endpoint(status_endpoint: Url) -> impl NodeMetadataFetcher {
    StatusEndpointVersionFetcher { status_endpoint }
}

#[derive(Clone)]
pub struct StatusEndpointVersionFetcher {
    status_endpoint: Url,
}

#[async_trait]
impl NodeMetadataFetcher for StatusEndpointVersionFetcher {
    async fn fetch(&self) -> Result<NodeMetadata, BuildVersionFetchError> {
        let status_endpoint = self.status_endpoint.clone();
        debug!("Fetching build version for {}", status_endpoint);
        match fetch_metadata_from_status(status_endpoint).await {
            Ok(metadata) => {
                metadata.validate()?;
                Ok(metadata)
            }
            Err(fetch_err) => Err(BuildVersionFetchError::Error(fetch_err)),
        }
    }
}

// Fetch the build version by requesting the status from the node's rest server.
async fn fetch_metadata_from_status(status_endpoint: Url) -> Result<NodeMetadata, Error> {
    let status_response = reqwest::get(status_endpoint)
        .await
        .context("Should have responded with status")?;

    // The exact structure of the response varies over the versions but there is an assertion made
    // that .build_version is always valid. So the key is accessed without deserialising the full response.
    let response_json: Value = status_response
        .json()
        .await
        .context("Should have parsed JSON from response")?;

    let build_version = try_resolve_version(&response_json)?;
    let network_name = try_resolve_network_name(&response_json)?;
    Ok(NodeMetadata {
        build_version,
        network_name,
    })
}

fn try_resolve_network_name(raw_response: &Value) -> Result<String, Error> {
    match raw_response.get(CHAINSPEC_NAME_KEY) {
        Some(build_version_value) if build_version_value.is_string() => {
            let raw = build_version_value
                .as_str()
                .context("chainspec_name should be a string")?;
            Ok(raw.to_string())
        }
        _ => {
            count_error("failed_getting_chainspec_name_from_node_status");
            Err(anyhow!(
                "failed to get {} from status response {}",
                CHAINSPEC_NAME_KEY,
                raw_response
            ))
        }
    }
}

fn try_resolve_version(raw_response: &Value) -> Result<ProtocolVersion, Error> {
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
            count_error("failed_getting_build_version_from_node_status");
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
    use serde_json::Map;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn try_resolve_version_should_interpret_correct_build_version() {
        let mut metadata = test_by_build_version(Some("5.1.111-b94c4f79a"), Some("network-1"))
            .await
            .unwrap();
        assert_eq!(
            metadata,
            NodeMetadata {
                build_version: ProtocolVersion::new(SemVer::new(5, 1, 111)),
                network_name: "network-1".to_string(),
            }
        );

        metadata =
            test_by_build_version(Some("6.2.112-b94c4f79a-casper-mainnet"), Some("network-2"))
                .await
                .unwrap();
        assert_eq!(
            metadata,
            NodeMetadata {
                build_version: ProtocolVersion::new(SemVer::new(6, 2, 112)),
                network_name: "network-2".to_string(),
            }
        );

        metadata = test_by_build_version(Some("7.3.113"), Some("network-3"))
            .await
            .unwrap();
        assert_eq!(
            metadata,
            NodeMetadata {
                build_version: ProtocolVersion::new(SemVer::new(7, 3, 113)),
                network_name: "network-3".to_string(),
            }
        );

        let version_validation_failed =
            test_by_build_version(Some("1.5.1"), Some("some-network")).await;
        assert!(matches!(
            version_validation_failed,
            Err(BuildVersionFetchError::VersionNotAcceptable(_))
        ));
    }

    #[tokio::test]
    async fn try_resolve_should_fail_if_build_version_is_absent() {
        let ret = test_by_build_version(None, Some("x")).await;
        assert!(ret.is_err());
    }

    #[tokio::test]
    async fn try_resolve_should_fail_if_build_version_is_invalid() {
        let ret = test_by_build_version(Some("not-a-semver"), Some("x")).await;
        assert!(ret.is_err());
    }

    #[tokio::test]
    async fn try_resolve_should_fail_if_no_network_name_in_response() {
        let ret = test_by_build_version(Some("2.0.0"), None).await;
        assert!(ret.is_err());
    }

    fn build_server_mock(
        build_version: Option<&str>,
        network_name: Option<&str>,
    ) -> (Mock, String, ServerGuard) {
        let mut server = Server::new();
        let url = format!("{}/status", server.url());
        let mut m = Map::new();
        if let Some(version) = build_version {
            m.insert(BUILD_VERSION_KEY.to_string(), version.to_string().into());
        }
        if let Some(network) = network_name {
            m.insert(CHAINSPEC_NAME_KEY.to_string(), network.to_string().into());
        }
        let json_object: Value = m.into();
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
        network_name: Option<&str>,
    ) -> Result<NodeMetadata, BuildVersionFetchError> {
        let (mock, url, _server) = build_server_mock(build_version, network_name);
        let result = for_status_endpoint(Url::parse(&url).unwrap()).fetch().await;
        mock.assert();
        result
    }

    pub struct MockVersionFetcher {
        repeatable: bool,
        version_responses: Mutex<Vec<Result<ProtocolVersion, BuildVersionFetchError>>>,
        network_name_responses: Mutex<Vec<Result<String, BuildVersionFetchError>>>,
    }

    impl MockVersionFetcher {
        pub fn repeatable_from_protocol_version(version: &str, network_name: &str) -> Self {
            let protocol_version = ProtocolVersion::from_str(version).unwrap();
            Self {
                repeatable: true,
                version_responses: Mutex::new(vec![Ok(protocol_version)]),
                network_name_responses: Mutex::new(vec![Ok(network_name.to_string())]),
            }
        }
        pub fn new(
            version_responses: Vec<Result<ProtocolVersion, BuildVersionFetchError>>,
            network_name_responses: Vec<Result<String, BuildVersionFetchError>>,
        ) -> Self {
            Self {
                repeatable: false,
                version_responses: Mutex::new(version_responses),
                network_name_responses: Mutex::new(network_name_responses),
            }
        }
    }

    #[async_trait]
    impl NodeMetadataFetcher for MockVersionFetcher {
        async fn fetch(&self) -> Result<NodeMetadata, BuildVersionFetchError> {
            let mut version_responses = self.version_responses.lock().await;
            let mut network_name_responses = self.network_name_responses.lock().await;
            if self.repeatable {
                let version = version_responses[0].clone()?;
                let network_name = network_name_responses[0].clone()?;
                return Ok(NodeMetadata {
                    build_version: version,
                    network_name,
                });
            }
            //If we are fetching something that wasn't prepared it should be an error
            let version = version_responses.pop().unwrap()?;
            let network_name = network_name_responses.pop().unwrap()?;
            Ok(NodeMetadata {
                build_version: version,
                network_name,
            })
        }
    }
}
