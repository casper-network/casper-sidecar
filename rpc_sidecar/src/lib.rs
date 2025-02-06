mod config;
mod http_server;
mod node_client;
mod rpcs;
mod speculative_exec_config;
mod speculative_exec_server;
#[cfg(any(feature = "testing", test))]
pub mod testing;

use anyhow::Error;
use casper_binary_port::{Command, CommandHeader};
use casper_types::{
    bytesrepr::{self, ToBytes},
    ProtocolVersion,
};
pub use config::{FieldParseError, NodeClientConfig, RpcConfig, RpcServerConfig};
use futures::{future::BoxFuture, FutureExt};
pub use http_server::run as run_rpc_server;
use hyper::{
    server::{conn::AddrIncoming, Builder as ServerBuilder},
    Server,
};
use node_client::FramedNodeClient;
pub use node_client::{Error as ClientError, NodeClient};
pub use speculative_exec_config::Config as SpeculativeExecConfig;
pub use speculative_exec_server::run as run_speculative_exec_server;
use std::{net::SocketAddr, process::ExitCode, sync::Arc};
use tracing::{error, warn};
/// Minimal casper protocol version supported by this sidecar.
pub const SUPPORTED_PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::from_parts(2, 0, 0);

/// The exit code is used to indicate that the client has shut down due to version mismatch.
pub const CLIENT_SHUTDOWN_EXIT_CODE: u8 = 0x3;

pub type MaybeRpcServerReturn<'a> = Result<Option<BoxFuture<'a, Result<ExitCode, Error>>>, Error>;
pub async fn build_rpc_server<'a>(
    config: RpcServerConfig,
    maybe_network_name: Option<String>,
) -> MaybeRpcServerReturn<'a> {
    let (node_client, reconnect_loop, keepalive_loop) =
        FramedNodeClient::new(config.node_client.clone(), maybe_network_name).await?;
    let node_client: Arc<dyn NodeClient> = node_client;
    let mut futures = Vec::new();
    let main_server_config = config.main_server;
    if main_server_config.enable_server {
        let future = run_rpc(main_server_config, node_client.clone())
            .map(|q| {
                if let Err(e) = q {
                    error!("Rpc server finished with error: {}", e);
                }
                Ok(ExitCode::SUCCESS)
            })
            .boxed();
        futures.push(future);
    }
    let speculative_server_config = config.speculative_exec_server;
    if let Some(config) = speculative_server_config {
        if config.enable_server {
            let future = run_speculative_exec(config, node_client.clone())
                .map(|q| {
                    if let Err(e) = q {
                        error!("Rpc speculative server finished with error: {}", e);
                    }
                    Ok(ExitCode::SUCCESS)
                })
                .boxed();
            futures.push(future);
        }
    }
    let reconnect_loop = reconnect_loop
        .map(|q| {
            if let Err(e) = q {
                error!("reconect_loop finished with error: {}", e);
            }
            Ok(ExitCode::from(CLIENT_SHUTDOWN_EXIT_CODE))
        })
        .boxed();
    futures.push(reconnect_loop);
    let keepalive_loop = keepalive_loop
        .map(|q| {
            if let Err(e) = q {
                error!("keepalive_loop finished with error: {}", e);
            }
            Ok(ExitCode::from(CLIENT_SHUTDOWN_EXIT_CODE))
        })
        .boxed();
    futures.push(keepalive_loop);
    Ok(Some(retype_future_vec(futures).boxed()))
}

async fn retype_future_vec(
    futures: Vec<BoxFuture<'_, Result<ExitCode, Error>>>,
) -> Result<ExitCode, Error> {
    futures::future::select_all(futures).await.0
}

async fn run_rpc(config: RpcConfig, node_client: Arc<dyn NodeClient>) -> Result<(), Error> {
    run_rpc_server(
        node_client,
        start_listening(&SocketAddr::new(config.ip_address, config.port))?,
        config.qps_limit,
        config.max_body_bytes,
        config.cors_origin.clone(),
    )
    .await;
    Ok(())
}

async fn run_speculative_exec(
    config: SpeculativeExecConfig,
    node_client: Arc<dyn NodeClient>,
) -> anyhow::Result<()> {
    run_speculative_exec_server(
        node_client,
        start_listening(&SocketAddr::new(config.ip_address, config.port))?,
        config.qps_limit,
        config.max_body_bytes,
        config.cors_origin.clone(),
    )
    .await;
    Ok(())
}

fn start_listening(address: &SocketAddr) -> anyhow::Result<ServerBuilder<AddrIncoming>> {
    Server::try_bind(address).map_err(|error| {
        warn!(%error, %address, "failed to start HTTP server");
        error.into()
    })
}

fn encode_request(req: &Command, id: u16) -> Result<Vec<u8>, bytesrepr::Error> {
    let header = CommandHeader::new(req.tag(), id);
    let mut bytes = Vec::with_capacity(header.serialized_length() + req.serialized_length());
    header.write_bytes(&mut bytes)?;
    req.write_bytes(&mut bytes)?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write;

    use assert_json_diff::{assert_json_eq, assert_json_matches_no_panic, CompareMode, Config};
    use regex::Regex;
    use serde_json::Value;

    use crate::rpcs::docs::OPEN_RPC_SCHEMA;

    use crate::rpcs::speculative_open_rpc_schema::SPECULATIVE_OPEN_RPC_SCHEMA;
    use crate::rpcs::{
        docs::OpenRpcSchema,
        info::{GetChainspecResult, GetStatusResult, GetValidatorChangesResult},
    };
    use schemars::schema_for;

    #[test]
    fn main_server_json_schema_check() {
        json_schema_check("rpc_schema.json", &OPEN_RPC_SCHEMA);
    }

    #[test]
    fn speculative_json_schema_check() {
        json_schema_check("speculative_rpc_schema.json", &SPECULATIVE_OPEN_RPC_SCHEMA);
    }

    #[test]
    fn json_schema_status_check() {
        let schema_path = format!(
            "{}/../resources/test/schema_status.json",
            env!("CARGO_MANIFEST_DIR")
        );
        assert_schema(
            &schema_path,
            &serde_json::to_string_pretty(&schema_for!(GetStatusResult)).unwrap(),
        );
    }

    #[test]
    fn json_schema_validator_changes_check() {
        let schema_path = format!(
            "{}/../resources/test/schema_validator_changes.json",
            env!("CARGO_MANIFEST_DIR")
        );
        assert_schema(
            &schema_path,
            &serde_json::to_string_pretty(&schema_for!(GetValidatorChangesResult)).unwrap(),
        );
    }

    #[test]
    fn json_schema_rpc_schema_check() {
        let schema_path = format!(
            "{}/../resources/test/schema_rpc_schema.json",
            env!("CARGO_MANIFEST_DIR")
        );
        assert_schema(
            &schema_path,
            &serde_json::to_string_pretty(&schema_for!(OpenRpcSchema)).unwrap(),
        );
    }

    #[test]
    fn json_schema_chainspec_bytes_check() {
        let schema_path = format!(
            "{}/../resources/test/schema_chainspec_bytes.json",
            env!("CARGO_MANIFEST_DIR")
        );
        assert_schema(
            &schema_path,
            &serde_json::to_string_pretty(&schema_for!(GetChainspecResult)).unwrap(),
        );
    }

    /// Assert that the file at `schema_path` matches the provided `actual_schema`, which can be
    /// derived from `schemars::schema_for!` or `schemars::schema_for_value!`, for example. This
    /// method will create a temporary file with the actual schema and print the location if it
    /// fails.
    pub fn assert_schema(schema_path: &str, actual_schema: &str) {
        let expected_schema = fs::read_to_string(schema_path).unwrap();
        let expected_schema: Value = serde_json::from_str(&expected_schema).unwrap();
        let mut temp_file = tempfile::Builder::new()
            .suffix(".json")
            .tempfile_in(env!("OUT_DIR"))
            .unwrap();
        temp_file.write_all(actual_schema.as_bytes()).unwrap();

        let actual_schema: Value = serde_json::from_str(actual_schema).unwrap();
        let (_file, temp_file_path) = temp_file.keep().unwrap();

        let result = assert_json_matches_no_panic(
            &actual_schema,
            &expected_schema,
            Config::new(CompareMode::Strict),
        );
        assert_eq!(
            result,
            Ok(()),
            "schema does not match:\nexpected:\n{}\nactual:\n{}\n",
            schema_path,
            temp_file_path.display()
        );
        assert_json_eq!(actual_schema, expected_schema);
    }

    fn json_schema_check(schema_filename: &str, rpc_schema: &OpenRpcSchema) {
        let schema_path = format!(
            "{}/../resources/test/{}",
            env!("CARGO_MANIFEST_DIR"),
            schema_filename,
        );
        assert_schema(
            &schema_path,
            &serde_json::to_string_pretty(rpc_schema).unwrap(),
        );

        let schema = fs::read_to_string(&schema_path).unwrap();

        // Check for the following pattern in the JSON as this points to a byte array or vec (e.g.
        // a hash digest) not being represented as a hex-encoded string:
        //
        // ```json
        // "type": "array",
        // "items": {
        //   "type": "integer",
        //   "format": "uint8",
        //   "minimum": 0.0
        // },
        // ```
        //
        // The type/variant in question (most easily identified from the git diff) might be easily
        // fixed via application of a serde attribute, e.g.
        // `#[serde(with = "serde_helpers::raw_32_byte_array")]`.  It will likely require a
        // schemars attribute too, indicating it is a hex-encoded string.  See for example
        // `TransactionInvocationTarget::Package::addr`.
        let regex = Regex::new(
            r#"\s*"type":\s*"array",\s*"items":\s*\{\s*"type":\s*"integer",\s*"format":\s*"uint8",\s*"minimum":\s*0\.0\s*\},"#
        ).unwrap();
        assert!(
            !regex.is_match(&schema),
            "seems like a byte array is not hex-encoded - see comment in `json_schema_check` for \
            further info"
        );
    }
}
