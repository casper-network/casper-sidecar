use crate::types::config::AdminApiServerConfig;
use crate::utils::{resolve_address, root_filter, Unexpected};
use anyhow::Error;
use casper_event_types::metrics::metrics_summary;
use hyper::Server;
use std::net::TcpListener;
use std::process::ExitCode;
use std::time::Duration;
use tower::{buffer::Buffer, make::Shared, ServiceBuilder};
use warp::Filter;
use warp::{Rejection, Reply};

const BIND_ALL_INTERFACES: &str = "0.0.0.0";
struct AdminServer {
    port: u16,
    max_concurrent_requests: u32,
    max_requests_per_second: u32,
}

impl AdminServer {
    pub async fn start(&self) -> Result<(), Error> {
        let api = root_filter().or(metrics_filter());
        let address = format!("{}:{}", BIND_ALL_INTERFACES, self.port);
        let socket_address = resolve_address(&address)?;
        let listener = TcpListener::bind(socket_address)?;

        let warp_service = warp::service(api);
        let tower_service = ServiceBuilder::new()
            .concurrency_limit(self.max_concurrent_requests as usize)
            .rate_limit(self.max_requests_per_second as u64, Duration::from_secs(1))
            .service(warp_service);

        Server::from_tcp(listener)?
            .serve(Shared::new(Buffer::new(tower_service, 50)))
            .await?;

        Err(Error::msg("Admin server shutting down"))
    }
}

pub async fn run_server(config: AdminApiServerConfig) -> Result<ExitCode, Error> {
    AdminServer {
        port: config.port,
        max_concurrent_requests: config.max_concurrent_requests,
        max_requests_per_second: config.max_requests_per_second,
    }
    .start()
    .await
    .map(|_| ExitCode::SUCCESS)
}

/// Return metrics data at a given time.
/// Return: prometheus-formatted metrics data.
/// Example: curl http://127.0.0.1:18887/metrics
fn metrics_filter() -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("metrics")
        .and(warp::get())
        .and_then(metrics_handler)
}

async fn metrics_handler() -> Result<impl Reply, Rejection> {
    let res_custom = metrics_summary()
        .map_err(|err| warp::reject::custom(Unexpected(Error::msg(err.to_string()))))?;

    Ok(res_custom)
}

#[cfg(test)]
mod tests {
    use crate::{admin_server::run_server, types::config::AdminApiServerConfig};
    use portpicker::pick_unused_port;
    use reqwest::Response;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn given_config_should_start_admin_server() {
        let port = pick_unused_port().unwrap();
        let request_url = format!("http://localhost:{}/metrics", port);
        let admin_config = AdminApiServerConfig {
            port,
            max_concurrent_requests: 1,
            max_requests_per_second: 1,
        };
        tokio::spawn(run_server(admin_config));

        let response = fetch_metrics_data(&request_url).await;
        let text = response.text().await.unwrap();
        assert!(text.contains("process_cpu_seconds_total"));
    }

    async fn fetch_metrics_data(request_url: &String) -> Response {
        reqwest::Client::new()
            .get(request_url)
            .send()
            .await
            .expect("Error requesting the /metrics endpoint")
    }
}
