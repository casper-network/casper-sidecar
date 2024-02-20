mod config;

use anyhow::{Context, Error};
use backtrace::Backtrace;
use casper_event_sidecar::{run as run_sse_sidecar, run_admin_server, run_rest_server, Database};
use casper_rpc_sidecar::start_rpc_server as run_rpc_sidecar;
use clap::Parser;
use config::{SidecarConfig, SidecarConfigTarget};
use futures::FutureExt;
use std::{
    env, fmt, io,
    panic::{self, PanicInfo},
    process::{self, ExitCode},
};
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;
use tracing::{field::Field, info};
use tracing_subscriber::{
    fmt::{format, format::Writer},
    EnvFilter,
};

const MAX_THREAD_COUNT: usize = 512;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct CmdLineArgs {
    /// Path to the TOML-formatted config file
    #[arg(short, long, value_name = "FILE")]
    path_to_config: String,
}

fn main() -> Result<ExitCode, Error> {
    // Install global collector for tracing
    init_logging()?;

    let args = CmdLineArgs::parse();

    let path_to_config = args.path_to_config;

    let config_serde = read_config(&path_to_config).context("Error constructing config")?;
    let config: SidecarConfig = config_serde.try_into()?;
    config.validate()?;
    info!("Configuration loaded");

    let max_worker_threads = config.max_thread_count.unwrap_or_else(num_cpus::get);
    let max_blocking_threads = config
        .max_thread_count
        .unwrap_or(MAX_THREAD_COUNT - max_worker_threads);
    panic::set_hook(Box::new(panic_hook));

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(max_worker_threads)
        .max_blocking_threads(max_blocking_threads)
        .build()
        .expect("Failed building sidecar runtime")
        .block_on(run(config))
}

pub fn read_config(config_path: &str) -> Result<SidecarConfigTarget, Error> {
    let toml_content =
        std::fs::read_to_string(config_path).context("Error reading config file contents")?;
    toml::from_str(&toml_content).context("Error parsing config into TOML format")
}

async fn run(config: SidecarConfig) -> Result<ExitCode, Error> {
    let maybe_database = if let Some(storage_config) = config.storage.as_ref() {
        Some(Database::build(storage_config).await?)
    } else {
        None
    };
    let admin_server = if let Some(config) = config.admin_api_server {
        run_admin_server(config.clone()).boxed()
    } else {
        std::future::pending().boxed()
    };
    let rest_server = if let (Some(rest_config), Some(database)) =
        (config.rest_api_server, maybe_database.clone())
    {
        run_rest_server(rest_config.clone(), database).boxed()
    } else {
        std::future::pending().boxed()
    };

    let sse_server = if let (Some(storage_config), Some(database), Some(sse_server_config)) =
        (config.storage, maybe_database, config.sse_server)
    {
        // If sse server is configured, both storage config and database must be "Some" here. This should be ensured by prior validation.
        run_sse_sidecar(
            sse_server_config,
            database.clone(),
            storage_config.get_storage_path(),
        )
        .boxed()
    } else {
        std::future::pending().boxed()
    };

    let rpc_server = config.rpc_server.as_ref().map_or_else(
        || std::future::pending().boxed(),
        |conf| run_rpc_sidecar(conf).boxed(),
    );

    let result = tokio::select! {
        result = admin_server => result,
        result = rest_server => result,
        result = sse_server => result,
        result = rpc_server => result,
    };
    if let Err(error) = &result {
        info!("The server has exited with an error: {}", error);
    };
    result
}

fn panic_hook(info: &PanicInfo) {
    let backtrace = Backtrace::new();

    eprintln!("{:?}", backtrace);

    // Print panic info
    if let Some(s) = info.payload().downcast_ref::<&str>() {
        eprintln!("sidecar panicked: {}", s);
    } else {
        eprintln!("{}", info);
    }
    process::abort()
}

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
