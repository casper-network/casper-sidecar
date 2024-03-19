# Casper Event Sidecar README

## Summary of Purpose

The Casper Event Sidecar is an application that runs in tandem with the node process. This reduces the load on the node process by allowing subscribers to monitor the event stream through the Sidecar while the node focuses entirely on the blockchain. Users needing access to the JSON-RPC will still need to query the node directly.

While the primary use case for the Sidecar application is running alongside the node on the same machine, it can be run remotely if necessary.

### System Components & Architecture

![Sidecar Diagram](/images/SidecarDiagram.png)

Casper Nodes offer a Node Event Stream API returning Server-Sent Events (SSEs) that hold JSON-encoded data. The SSE Sidecar uses this API to achieve the following goals:

* Build a sidecar middleware service that reads the Event Stream of all connected nodes, acting as a passthrough and replicating the SSE interface of the connected nodes and their filters (i.e., `/main`, `/deploys`, and `/sigs` with support for the use of the `?start_from=` query to allow clients to get previously sent events from the Sidecar's buffer).

* Provide a new RESTful endpoint that is discoverable to node operators. See the [usage instructions](USAGE.md) for details.

The SSE Sidecar uses one ring buffer for outbound events, providing some robustness against unintended subscriber disconnects. If a disconnected subscriber re-subscribes before the buffer moves past their last received event, there will be no gap in the event history if they use the `start_from` URL query.

## Prerequisites

* CMake 3.1.4 or greater
* [Rust](https://www.rust-lang.org/tools/install)
* pkg-config
* gcc
* g++

## Configuration

The SSE Sidecar service must be configured using a `.toml` file specified at runtime.

This repository contains several sample configuration files that can be used as examples and adjusted according to your scenario:

- [EXAMPLE_NCTL_CONFIG.toml](./resources/example_configs/EXAMPLE_NCTL_CONFIG.toml) - Configuration for connecting to nodes on a local NCTL network. This configuration is used in the unit and integration tests found in this repository
- [EXAMPLE_NCTL_POSTGRES_CONFIG.toml](./resources/example_configs/EXAMPLE_NCTL_POSTGRES_CONFIG.toml) - Configuration for using the PostgreSQL database and nodes on a local NCTL network
- [EXAMPLE_NODE_CONFIG.toml](./resources/example_configs/EXAMPLE_NODE_CONFIG.toml) - Configuration for connecting to live nodes on a Casper network and setting up an admin server    

Once you create the configuration file and are ready to run the Sidecar service, you must provide the configuration as an argument using the `-- --path-to-config` option as described [here](#running-the-sidecar).

### Node Connections

The Sidecar can connect to Casper nodes with versions greater or equal to `2.0.0`.

The `node_connections` option configures the node (or multiple nodes) to which the Sidecar will connect and the parameters under which it will operate with that node. Connecting to multiple nodes requires multiple `[[sse_server.connections]]` sections.

```
[[sse_server.connections]]
ip_address = "127.0.0.1"
sse_port = 18101
rest_port = 14101
max_attempts = 10
delay_between_retries_in_seconds = 5
allow_partial_connection = false
enable_logging = true
connection_timeout_in_seconds = 3
no_message_timeout_in_seconds = 60
sleep_between_keep_alive_checks_in_seconds = 30

[[sse_server.connections]]
ip_address = "127.0.0.1"
sse_port = 18102
rest_port = 14102
max_attempts = 10
delay_between_retries_in_seconds = 5
allow_partial_connection = false
enable_logging = false
connection_timeout_in_seconds = 3
no_message_timeout_in_seconds = 60
sleep_between_keep_alive_checks_in_seconds = 30

[[sse_server.connections]]
ip_address = "127.0.0.1"
sse_port = 18103
rest_port = 14103
max_attempts = 10
delay_between_retries_in_seconds = 5
allow_partial_connection = false
enable_logging = false
connection_timeout_in_seconds = 3
no_message_timeout_in_seconds = 60
sleep_between_keep_alive_checks_in_seconds = 30
```

* `ip_address` - The IP address of the node to monitor.
* `sse_port` - The node's event stream (SSE) port. This [example configuration](./resources/example_configs/EXAMPLE_NODE_CONFIG.toml) uses port `9999`.
* `rest_port` - The node's REST endpoint for status and metrics. This [example configuration](./resources/example_configs/EXAMPLE_NODE_CONFIG.toml) uses port `8888`.
* `max_attempts` - The maximum number of attempts the Sidecar will make to connect to the node. If set to `0`, the Sidecar will not attempt to connect.
* `delay_between_retries_in_seconds` - The delay between attempts to connect to the node.
* `allow_partial_connection` - Determining whether the Sidecar will allow a partial connection to this node.
* `enable_logging` - This enables the logging of events from the node in question.
* `connection_timeout_in_seconds` - Number of seconds before the connection request times out. Parameter is optional, defaults to 5
* `no_message_timeout_in_seconds` - Number of seconds after which the connection will be restarted if no bytes were received. Parameter is optional, defaults to 120
* `sleep_between_keep_alive_checks_in_seconds` - Optional parameter specifying the time intervals (in seconds) for checking if the connection is still alive. Defaults to 60

### Storage

This directory stores the SSE cache and an SQLite database if the Sidecar is configured to use SQLite.

```
[storage]
storage_path = "./target/storage"
```

### Database Connectivity

The Sidecar can connect to different types of databases. The current options are `SQLite` or `PostgreSQL`. The following sections show how to configure the database connection for one of these DBs. Note that the Sidecar can only connect to one DB at a time.

#### SQLite Database

This section includes configurations for the SQLite database.

```
[storage.sqlite_config]
file_name = "sqlite_database.db3"
max_connections_in_pool = 100
# https://www.sqlite.org/compile.html#default_wal_autocheckpoint
wal_autocheckpointing_interval = 1000
```

* `file_name` - The database file path.
* `max_connections_in_pool` - The maximum number of connections to the database. (Should generally be left as is.)
* `wal_autocheckpointing_interval` - This controls how often the system commits pages to the database. The value determines the maximum number of pages before forcing a commit. More information can be found [here](https://www.sqlite.org/compile.html#default_wal_autocheckpoint).

#### PostgreSQL Database

The properties listed below are elements of the PostgreSQL database connection that can be configured for the Sidecar.

* `database_name` - Name of the database.
* `host` - URL to PostgreSQL instance.
* `database_username` - Username.
* `database_password` - Database password.
* `max_connections_in_pool` - The maximum number of connections to the database.
* `port` - The port for the database connection.


To run the Sidecar with PostgreSQL, you can set the following database environment variables to control how the Sidecar connects to the database. This is the suggested method to set the connection information for the PostgreSQL database.

```
SIDECAR_POSTGRES_USERNAME="your username"
```

```
SIDECAR_POSTGRES_PASSWORD="your password"
```

```
SIDECAR_POSTGRES_DATABASE_NAME="your database name"
```

```
SIDECAR_POSTGRES_HOST="your host"
```

```
SIDECAR_POSTGRES_MAX_CONNECTIONS="max connections"
```

```
SIDECAR_POSTGRES_PORT="port"
```

However, DB connectivity can also be configured using the Sidecar configuration file.

If the DB environment variables and the Sidecar's configuration file have the same variable set, the DB environment variables will take precedence.

It is possible to completely omit the PostgreSQL configuration from the Sidecar's configuration file. In this case, the Sidecar will attempt to connect to the PostgreSQL using the database environment variables or use some default values for non-critical variables.

```
[storage.postgresql_config]
database_name = "event_sidecar"
host = "localhost"
database_password = "p@$$w0rd"
database_username = "postgres"
max_connections_in_pool = 30
```

### Rest & Event Stream Criteria

This information determines outbound connection criteria for the Sidecar's `rest_server`.

```
[rest_api_server]
port = 18888
max_concurrent_requests = 50
max_requests_per_second = 50
request_timeout_in_seconds = 10
```

* `port` - The port for accessing the sidecar's `rest_server`. `18888` is the default, but operators are free to choose their own port as needed.
* `max_concurrent_requests` - The maximum total number of simultaneous requests that can be made to the REST server.
* `max_requests_per_second` - The maximum total number of requests that can be made per second.
* `request_timeout_in_seconds` - The total time before a request times out.

```
[sse_server.event_stream_server]
port = 19999
max_concurrent_subscribers = 100
event_stream_buffer_length = 5000
```

The `event_stream_server` section specifies a port for the Sidecar's event stream.

Additionally, there are the following two options:

* `max_concurrent_subscribers` - The maximum number of subscribers that can monitor the Sidecar's event stream.
* `event_stream_buffer_length` - The number of events that the stream will hold in its buffer for reference when a subscriber reconnects.

### Admin Server

This optional section configures the Sidecar's administrative server. If this section is not specified, the Sidecar will not start an admin server.

```
[admin_api_server]
port = 18887
max_concurrent_requests = 1
max_requests_per_second = 1
```

* `port` - The port for accessing the Sidecar's admin server.
* `max_concurrent_requests` - The maximum total number of simultaneous requests that can be sent to the admin server.
* `max_requests_per_second` - The maximum total number of requests that can be sent per second to the admin server.

Access the admin server at `http://localhost:18887/metrics/`.

## Swagger Documentation

Once the Sidecar is running, access the Swagger documentation at `http://localhost:18888/swagger-ui/`. You need to replace `localhost` with the IP address of the machine running the Sidecar application if you are running the Sidecar remotely. The Swagger documentation will allow you to test the REST API.

## OpenAPI Specification

An OpenAPI schema is available at `http://localhost:18888/api-doc.json/`. You need to replace `localhost` with the IP address of the machine running the Sidecar application if you are running the Sidecar remotely.

## Unit Testing the Sidecar

You can run the unit and integration tests included in this repository with the following command:

```
cargo test
```

You can also run the performance tests using the following command:

```
cargo test -- --include-ignored
```

The [EXAMPLE_NCTL_CONFIG.toml](./resources/example_configs/EXAMPLE_NCTL_CONFIG.toml) file contains the configurations used for these tests.

## Running the Sidecar

After creating the configuration file, run the Sidecar using Cargo and point to the configuration file using the `--path-to-config` option, as shown below. The command needs to run with `root` privileges.

```shell
sudo cargo run -- --path-to-config ./resources/example_configs/EXAMPLE_NODE_CONFIG.toml
```

The Sidecar application leverages tracing, which can be controlled by setting the `RUST_LOG` environment variable.

The following command will run the sidecar application with the `INFO` log level.

```
RUST_LOG=info cargo run -p casper-sidecar -- --path-to-config ./resources/example_configs/EXAMPLE_NCTL_CONFIG.toml
```

The log levels, listed in order of increasing verbosity, are:

* `ERROR`
* `WARN`
* `INFO`
* `DEBUG`
* `TRACE`

Further details about log levels can be found [here](https://docs.rs/env_logger/0.9.1/env_logger/#enabling-logging).

## Testing the Sidecar using NCTL

The Sidecar application can be tested against live Casper nodes or a local [NCTL network](https://docs.casperlabs.io/dapp-dev-guide/building-dapps/setup-nctl/).

The configuration shown within this README will direct the Sidecar application to a locally hosted NCTL network if one is running. The Sidecar should function the same way it would with a live node, displaying events as they occur in the local NCTL network.

## Troubleshooting Tips

This section covers helpful tips when troubleshooting the Sidecar service. Replace the URL and ports provided in the examples as appropriate.

### Checking liveness

To check whether the Sidecar is running, run the following `curl` command, which returns the newest stored block.

```sh
curl http://SIDECAR_URL:SIDECAR_REST_PORT/block
```

Each block should have a `.block.header.timestamp` field. Even if there were no deploys, a block should be produced every 30-60 seconds. If the latest block falls behind, it means there is an issue with the Sidecar reading events from the node. Here is a helpful script provided `jq` is installed:

```sh
curl http://SIDECAR_URL:SIDECAR_REST_PORT/block | jq '.block.header.timestamp'
```

### Checking the node connection

Checking the node connection status requires the admin server to be enabled, as shown [here](#admin-server). Use this `curl` command and observe the output:

```sh
curl http://SIDECAR_URL:SIDECAR_ADMIN_PORT/metrics
```

**Sample output**:

```
# HELP node_statuses Current status of node to which sidecar is connected. Numbers mean: 0 - preparing; 1 - connecting; 2 - connected; 3 - reconnecting; -1 - connections_exhausted -> used up all connection attempts ; -2 - incompatible -> node is in an incompatible version
# TYPE node_statuses gauge
node_statuses{node="35.180.42.211:9999"} 2
node_statuses{node="69.197.42.27:9999"} 2
```

In the above `node_statuses`, you can see which nodes are connecting, which are already connected, which are disconnected due to no more retries, etc. The number next to each node represents the connection status:

- `0` - The Sidecar is preparing to connect
- `1` - The Sidecar is connecting to the node
- `2` - The Sidecar is connected to this node
- `3` - The Sidecar is reconnecting
- `-1` - The Sidecar is not connected and has reached the maximum connection attempts
- `-2` - The Sidecar is not connected due to an incompatible node version


### Diagnosing errors


To diagnose errors, look for `error` logs and check the `error_counts` on the metrics page, `http://SIDECAR_URL:SIDECAR_ADMIN_PORT/metrics`, where most of the errors related to data flow will be stored:

```
# HELP error_counts Error counts
# TYPE error_counts counter
error_counts{category="connection_manager",description="fetching_from_stream_failed"} 6
```

### Monitoring memory consumption

To monitor the Sidecar's memory consumption, observe the metrics page, `http://SIDECAR_URL:SIDECAR_ADMIN_PORT/metrics`. Search for `process_resident_memory_bytes`:

```
# HELP process_resident_memory_bytes Resident memory size in bytes.
# TYPE process_resident_memory_bytes gauge
process_resident_memory_bytes 292110336
```

If memory consumption is high without an apparent reason, please inform the Sidecar team by creating an [issue in GitHub](https://github.com/casper-network/casper-sidecar/issues).

Remember to check the `event_stream_buffer_length` setting in the configuration because it dramatically impacts how much memory the Sidecar consumes. Also, some events, like step events, consume more memory.

### Ensuring sufficient storage

Ensuring enough space in the database is essential for the Sidecar to consume events produced from the nodes' SSE streams over a more extended period. Each event is written to the database in a raw format for future processing. Running the Sidecar for an extended period (weeks or months) can result in storing multiple Gigabytes of data. If the database runs out of space, the Sidecar will lose events, as it cannot record them.

### Inspecting the REST API

The easiest way to inspect the Sidecar’s REST API is with [Swagger](#swagger-documentation).

### Limiting concurrent requests

The Sidecar can be configured to limit concurrent requests (`max_concurrent_requests`) and requests per second (`max_requests_per_second`) for the REST and admin servers.

However, remember that those are application-level guards, meaning that the operating system already accepted the connection, which used up the operating system's resources. Limiting potential DDoS attacks requires consideration before the requests are directed to the Sidecar application.
