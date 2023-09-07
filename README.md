# Casper Event Sidecar README

## Summary of Purpose

The Casper Event Sidecar is an application that runs in tandem with the node process. This reduces the load on the node process by allowing subscribers to monitor the event stream through the Sidecar while the node focuses entirely on the blockchain. Users needing access to the JSON-RPC will still need to query the node directly.

While the primary use case for the Sidecar application is running alongside the node on the same machine, it can be run remotely if necessary.

### System Components & Architecture

![Sidecar Diagram](/images/SidecarDiagram.png)

Casper Nodes offer a Node Event Stream API returning Server-Sent Events (SSEs) that hold JSON-encoded data. The SSE Sidecar uses this API to achieve the following goals:

* Build a sidecar middleware service that connects to the Node Event Stream, with a passthrough that replicates the SSE interface of the node and its filters (i.e., `/main`, `/deploys`, and `/sigs` with support for the use of the `?start_from=` query to allow clients to get previously sent events from the Sidecar's buffer.)

* Provide a new RESTful endpoint that is discoverable to node operators. See the [usage instructions](USAGE.md) for details.

The SSE Sidecar uses one ring buffer for outbound events, providing some robustness against unintended subscriber disconnects. If a disconnected subscriber re-subscribes before the buffer moves past their last received event, there will be no gap in the event history if they use the `start_from` URL query.

## Prerequisites

* CMake 3.1.4 or greater
* [Rust](https://www.rust-lang.org/tools/install)
* pkg-config
* gcc
* g++

## Configuration

The file *example_config.toml* in the base *event-sidecar* directory contains default configuration details for your instance of the Sidecar application. These must be adjusted before running the application.

### Node Connections

```
[[connections]]
ip_address = "127.0.0.1"
sse_port = 18101
rest_port = 14101
max_attempts = 10
delay_between_retries_in_seconds = 5
allow_partial_connection = false
enable_logging = true

[[connections]]
ip_address = "127.0.0.1"
sse_port = 18102
rest_port = 14102
max_attempts = 10
delay_between_retries_in_seconds = 5
allow_partial_connection = false
enable_logging = false

[[connections]]
ip_address = "127.0.0.1"
sse_port = 18103
rest_port = 14103
max_attempts = 10
delay_between_retries_in_seconds = 5
allow_partial_connection = false
enable_logging = false
connection_timeout_in_seconds = 3
```

The `node_connections` option configures the node (or multiple nodes) to which the Sidecar will connect and the parameters under which it will operate with that node.

* `ip_address` - The IP address of the node to monitor.
* `sse_port` - The node's event stream (SSE) port. This [example configuration](EXAMPLE_NODE_CONFIG.toml) uses port `9999`.
* `rest_port` - The node's REST endpoint for status and metrics. This [example configuration](EXAMPLE_NODE_CONFIG.toml) uses port `8888`.
* `max_attempts` - The maximum number of attempts the Sidecar will make to connect to the node. If set to `0`, the Sidecar will not attempt to connect.
* `delay_between_retries_in_seconds` - The delay between attempts to connect to the node.
* `allow_partial_connection` - Determining whether the Sidecar will allow a partial connection to this node.
* `enable_logging` - This enables the logging of events from the node in question.
* `connection_timeout_in_seconds` - The total time before the connection request times out.

### Storage

This directory stores the SQLite database for the Sidecar and the SSE cache.

```
[storage]
storage_path = "./target/storage"
```

### SQLite Database

```
[storage.sqlite_config]
file_name = "sqlite_database.db3"
max_connections_in_pool = 100
# https://www.sqlite.org/compile.html#default_wal_autocheckpoint
wal_autocheckpointing_interval = 1000
```

This section includes configurations for the SQLite database.

* `file_name` - The database file path.
* `max_connections_in_pool` - The maximum number of connections to the database. (Should generally be left as is.)
* `wal_autocheckpointing_interval` - This controls how often the system commits pages to the database. The value determines the maximum number of pages before forcing a commit. More information can be found [here](https://www.sqlite.org/compile.html#default_wal_autocheckpoint).

### Rest & Event Stream Criteria

```
[rest_server]
port = 18888
max_concurrent_requests = 50
max_requests_per_second = 50
request_timeout_in_seconds = 10
```

This information determines outbound connection criteria for the Sidecar's `rest_server`.

* `port` - The port for accessing the sidecar's `rest_server`. `18888` is the default, but operators are free to choose their own port as needed.
* `max_concurrent_requests` - The maximum total number of simultaneous requests that can be made to the REST server.
* `max_requests_per_second` - The maximum total number of requests that can be made per second.
* `request_timeout_in_seconds` - The total time before a request times out.

```
[event_stream_server]
port = 19999
max_concurrent_subscribers = 100
event_stream_buffer_length = 5000
```

The `event_stream_server` section specifies a port for the Sidecar's event stream.

Additionally, there are the following two options:

* `max_concurrent_subscribers` - The maximum number of subscribers that can monitor the Sidecar's event stream.
* `event_stream_buffer_length` - The number of events that the stream will hold in its buffer for reference when a subscriber reconnects.

### Admin server

```
[admin_server]
port = 18887
max_concurrent_requests = 1
max_requests_per_second = 1
```

This information determines configuration for the Sidecar's `admin_server`. It is optional - if this section of configuration isn't specified then sidecar will not start an admin server.

* `port` - The port for accessing the sidecar's `admin_server`.
* `max_concurrent_requests` - The maximum total number of simultaneous requests that can be made to the REST server.
* `max_requests_per_second` - The maximum total number of requests that can be made per second.

## Unit Testing the Sidecar Application

You can run included unit tests with the following command:

```
cargo test
```

You can also run the integration and performance tests using the following command:

```
cargo test -- --include-ignored
```

## Running the Sidecar

Once you are happy with the configuration, you can run it using Cargo:

```shell
cargo run
```

The Sidecar application leverages tracing, which can be controlled by setting the `RUST_LOG` environment variable.

The following command will run the sidecar application with the `INFO` log level.

```
RUST_LOG=info cargo run -p casper-event-sidecar -- -p "EXAMPLE_NCTL_CONFIG.toml"
```

The log levels, listed in order of increasing verbosity, are:

* `ERROR`
* `WARN`
* `INFO`
* `DEBUG`
* `TRACE`

Further details can be found [here](https://docs.rs/env_logger/0.9.1/env_logger/#enabling-logging).

Additionally, to run the sidecar with PostgreSQL, you can set the following environment variables to control how the sidecar connects to the database:

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

The suggested method to set the connection information for the postgeSQL database is through these environment variables.
However, they can also be set in the configuration file

## Testing Sidecar with a Local Network using NCTL

Your instance of the Sidecar application can be tested against a local network using NCTL.

Instructions for setting up NCTL can be found [here](https://docs.casperlabs.io/dapp-dev-guide/building-dapps/setup-nctl/).

The configuration shown within this README will direct the Sidecar application to a locally hosted NCTL network if one is running. The Sidecar should function the same way it would with a live node, displaying events as they occur in the local NCTL network.
