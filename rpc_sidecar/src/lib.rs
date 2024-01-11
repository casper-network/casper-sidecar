mod config;
mod http_server;
mod node_client;
mod rpcs;
mod speculative_exec_config;
mod speculative_exec_server;
use anyhow::Error;
pub use config::RpcServerConfig;
pub use config::{NodeClientConfig, RpcConfig};
use futures::future::BoxFuture;
pub use http_server::run as run_rpc_server;
use hyper::{
    server::{conn::AddrIncoming, Builder as ServerBuilder},
    Server,
};
pub use node_client::{Error as ClientError, JulietNodeClient, NodeClient};
pub use speculative_exec_config::Config as SpeculativeExecConfig;
pub use speculative_exec_server::run as run_speculative_exec_server;
use std::{
    net::{SocketAddr, ToSocketAddrs},
    sync::Arc,
};
use tracing::warn;

pub async fn start_rpc_server(config: RpcServerConfig) -> Result<(), Error> {
    let (node_client, client_loop) = JulietNodeClient::new(&config.node_client).await;
    let node_client: Arc<dyn NodeClient> = Arc::new(node_client);

    let rpc_server = run_rpc(config.rpc_server, node_client.clone());

    let spec_exec_server = if let Some(spec_exec_config) = config.speculative_exec_server.as_ref() {
        Box::pin(run_speculative_exec(
            spec_exec_config.clone(),
            node_client.clone(),
        )) as BoxFuture<_>
    } else {
        Box::pin(ok_empty_run()) as BoxFuture<_>
    };

    let (rpc_result, spec_exec_result, ()) =
        tokio::join!(rpc_server, spec_exec_server, client_loop);
    rpc_result.and(spec_exec_result)
}

async fn run_rpc(config: RpcConfig, node_client: Arc<dyn NodeClient>) -> Result<(), Error> {
    run_rpc_server(
        node_client,
        start_listening(&config.address)?,
        config.qps_limit,
        config.max_body_bytes,
        config.cors_origin.clone(),
    )
    .await;
    Ok(())
}

async fn ok_empty_run() -> Result<(), Error> {
    Ok(())
}
async fn run_speculative_exec(
    config: SpeculativeExecConfig,
    node_client: Arc<dyn NodeClient>,
) -> anyhow::Result<()> {
    run_speculative_exec_server(
        node_client,
        start_listening(&config.address)?,
        config.qps_limit,
        config.max_body_bytes,
        config.cors_origin.clone(),
    )
    .await;
    Ok(())
}

fn start_listening(address: &str) -> anyhow::Result<ServerBuilder<AddrIncoming>> {
    let address = resolve_address(address).map_err(|error| {
        warn!(%error, %address, "failed to start HTTP server, cannot parse address");
        error
    })?;

    Server::try_bind(&address).map_err(|error| {
        warn!(%error, %address, "failed to start HTTP server");
        error.into()
    })
}

/// Parses a network address from a string, with DNS resolution.
fn resolve_address(address: &str) -> anyhow::Result<SocketAddr> {
    address
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow::anyhow!("failed to resolve address"))
}
/*
fn load_config(config: &Path) -> anyhow::Result<RpcServerConfig> {
    // The app supports running without a config file, using default values.
    let encoded_config = fs::read_to_string(config)
        .context("could not read configuration file")
        .with_context(|| config.display().to_string())?;

    // Get the TOML table version of the config indicated from CLI args, or from a new
    // defaulted config instance if one is not provided.
    let config = toml::from_str(&encoded_config)?;

    Ok(config)
}

/// Aborting panic hook.
///
/// Will exit the application using `abort` when an error occurs. Always shows a backtrace.
fn panic_hook(info: &PanicInfo) {
    let backtrace = Backtrace::new();

    eprintln!("{:?}", backtrace);

    // Print panic info
    if let Some(s) = info.payload().downcast_ref::<&str>() {
        eprintln!("node panicked: {}", s);
    // TODO - use `info.message()` once https://github.com/rust-lang/rust/issues/66745 is fixed
    // } else if let Some(message) = info.message() {
    //     eprintln!("{}", message);
    } else {
        eprintln!("{}", info);
    }

    // Abort after a panic, even if only a worker thread panicked.
    process::abort()
}

/// Initializes the logging system.
///
/// This function should only be called once during the lifetime of the application. Do not call
/// this outside of the application or testing code, the installed logger is global.
#[allow(trivial_casts)]
fn init_logging() -> anyhow::Result<()> {
    const LOG_CONFIGURATION_ENVVAR: &str = "RUST_LOG";

    const LOG_FIELD_MESSAGE: &str = "message";
    const LOG_FIELD_TARGET: &str = "log.target";
    const LOG_FIELD_MODULE: &str = "log.module_path";
    const LOG_FIELD_FILE: &str = "log.file";
    const LOG_FIELD_LINE: &str = "log.line";

    type FormatDebugFn = fn(&mut Writer, &Field, &dyn std::fmt::Debug) -> fmt::Result;

    fn format_into_debug_writer(
        writer: &mut Writer,
        field: &Field,
        value: &dyn fmt::Debug,
    ) -> fmt::Result {
        match field.name() {
            LOG_FIELD_MESSAGE => write!(writer, "{:?}", value),
            LOG_FIELD_TARGET | LOG_FIELD_MODULE | LOG_FIELD_FILE | LOG_FIELD_LINE => Ok(()),
            _ => write!(writer, "; {}={:?}", field, value),
        }
    }

    let formatter = format::debug_fn(format_into_debug_writer as FormatDebugFn);

    let filter = EnvFilter::new(
        env::var(LOG_CONFIGURATION_ENVVAR)
            .as_deref()
            .unwrap_or("warn,casper_rpc_sidecar=info"),
    );

    let builder = tracing_subscriber::fmt()
        .with_writer(io::stdout as fn() -> io::Stdout)
        .with_env_filter(filter)
        .fmt_fields(formatter)
        .with_filter_reloading();
    builder.try_init().map_err(|error| anyhow::anyhow!(error))?;
    Ok(())
}

// Note: The docstring on `Cli` is the help shown when calling the binary with `--help`.
#[derive(Debug, StructOpt)]
#[allow(rustdoc::invalid_html_tags)]
/// Casper blockchain node.
pub struct Cli {
    /// Path to configuration file.
    config: PathBuf,
}
*/
#[cfg(test)]
mod tests {
    use std::fs;

    use assert_json_diff::{assert_json_eq, assert_json_matches_no_panic, CompareMode, Config};
    use regex::Regex;
    use serde_json::Value;
    use std::io::Write;

    use crate::rpcs::docs::OPEN_RPC_SCHEMA;

    use crate::rpcs::{
        docs::OpenRpcSchema,
        info::{GetChainspecResult, GetStatusResult, GetValidatorChangesResult},
    };
    use schemars::schema_for;

    #[test]
    fn json_schema_check() {
        let schema_path = format!(
            "{}/../resources/test/rpc_schema.json",
            env!("CARGO_MANIFEST_DIR")
        );
        assert_schema(
            &schema_path,
            &serde_json::to_string_pretty(&*OPEN_RPC_SCHEMA).unwrap(),
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
}
