pub mod component;
mod config;
mod run;

use anyhow::{Context, Error};
use backtrace::Backtrace;
use clap::Parser;
use config::{SidecarConfig, SidecarConfigTarget};
use run::run;
use std::{
    env, fmt, io,
    panic::{self, PanicHookInfo},
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

fn panic_hook(info: &PanicHookInfo) {
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
