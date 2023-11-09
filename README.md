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

- [EXAMPLE_NCTL_CONFIG.toml](./EXAMPLE_NCTL_CONFIG.toml) - Configuration for connecting to nodes on a local NCTL network. This configuration is used in the unit and integration tests found in this repository
- [EXAMPLE_NCTL_POSTGRES_CONFIG.toml](./EXAMPLE_NCTL_POSTGRES_CONFIG.toml) - Configuration for using the PostgreSQL database and nodes on a local NCTL network
- [EXAMPLE_NODE_CONFIG.toml](./EXAMPLE_NODE_CONFIG.toml) - Configuration for connecting to live nodes on a Casper network and setting up an admin server

Once you create the configuration file and are ready to run the Sidecar service, you must provide the configuration as an argument using the `-- --path-to-config` option as described [here](#running-the-sidecar).

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
no_message_timeout_in_seconds = 60
sleep_between_keep_alive_checks_in_seconds = 30
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
* `no_message_timeout_in_seconds` - Number of seconds after which the connection will be restarted if no bytes were received. Parameter is optional, defaults to 120
* `sleep_between_keep_alive_checks_in_seconds` - Optional parameter specifying the time intervals (in seconds) for checking if the connection is still alive. Defaults to 60

### Storage

This directory stores the SSE cache and an SQLite database if configured to use SQLite.

```
[storage]
storage_path = "./target/storage"
```

### Database Connectivity

The Sidecar can connect to different types of databases. The current options are `SQLite` or `PostgreSQL`. The following sections show how to configure the database connection for one of these DBs. Note that the Sidecar can only connect to one DB at a time.

#### SQLite Database

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

### Admin Server

This optional section configures the Sidecar's administrative server. If this section is not specified, the Sidecar will not start an admin server.

```
[admin_server]
port = 18887
max_concurrent_requests = 1
max_requests_per_second = 1
```

* `port` - The port for accessing the Sidecar's admin server.
* `max_concurrent_requests` - The maximum total number of simultaneous requests that can be sent to the admin server.
* `max_requests_per_second` - The maximum total number of requests that can be sent per second to the admin server.

You can access the admin server at `http://localhost:18887/metrics/`.

## Swagger Documentation

Once the Sidecar is running, you can access the Swagger documentation at `http://localhost:18888/swagger-ui/`. You will need to replace `localhost` with the IP address of the machine running the Sidecar application if you are running the Sidecar remotely. The Swagger documentation will allow you to test the REST API.

## OpenAPI Specification

An OpenAPI schema is available at the following URL: http://localhost:18888/api-doc.json/. You will need to replace `localhost` with the IP address of the machine running the Sidecar application if you are running the Sidecar remotely.

## Unit Testing the Sidecar

You can run the unit and integration tests included in this repository with the following command:

```
cargo test
```

You can also run the performance tests using the following command:

```
cargo test -- --include-ignored
```

The [EXAMPLE_NCTL_CONFIG.toml](./EXAMPLE_NCTL_CONFIG.toml) file contains the configurations used for these tests.

## Running the Sidecar

After creating the configuration file, run the Sidecar using Cargo and point to the configuration file using the `--path-to-config` option, as shown below. The command needs to run with `root` privileges.

```shell
sudo cargo run -- --path-to-config "EXAMPLE_NODE_CONFIG.toml"
```

The Sidecar application leverages tracing, which can be controlled by setting the `RUST_LOG` environment variable.

The following command will run the sidecar application with the `INFO` log level.

```
RUST_LOG=info cargo run -p casper-event-sidecar -- --path-to-config "EXAMPLE_NCTL_CONFIG.toml"
```

The log levels, listed in order of increasing verbosity, are:

* `ERROR`
* `WARN`
* `INFO`
* `DEBUG`
* `TRACE`

Further details about log levels can be found [here](https://docs.rs/env_logger/0.9.1/env_logger/#enabling-logging).

## Testing Sidecar with a Local Network using NCTL

The Sidecar application can be tested against live Casper nodes or a local [NCTL network](https://docs.casperlabs.io/dapp-dev-guide/building-dapps/setup-nctl/).

The configuration shown within this README will direct the Sidecar application to a locally hosted NCTL network if one is running. The Sidecar should function the same way it would with a live node, displaying events as they occur in the local NCTL network.
