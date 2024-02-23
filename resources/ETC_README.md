# Casper Event Sidecar README for Node Operators

## Summary of Purpose

The Casper Event Sidecar is an application that runs in tandem with the node process. This reduces the load on the node process by allowing subscribers to monitor the event stream through the Sidecar, while the node focuses entirely on the blockchain. Users needing access to the JSON-RPC will still need to query the node directly.

While the primary use case for the Sidecar application is running alongside the node on the same machine, it can be run remotely if necessary.

### System Components & Architecture

Casper Nodes offer a Node Event Stream API returning Server-Sent Events (SSEs) that hold JSON-encoded data. The SSE Sidecar uses this API to achieve the following goals:

* Build a sidecar middleware service that reads the Event Stream of all connected nodes, acting as a passthrough and replicating the SSE interface of the connected nodes and their filters (i.e., `/main`, `/deploys`, and `/sigs` with support for the use of the `?start_from=` query to allow clients to get previously sent events from the Sidecar's buffer).

* Provide a new RESTful endpoint that is discoverable to node operators.

The SSE Sidecar uses one ring buffer for outbound events, providing some robustness against unintended subscriber disconnects. If a disconnected subscriber re-subscribes before the buffer moves past their last received event, there will be no gap in the event history if they use the `start_from` URL query.


## Configuration

The file `/etc/casper-event-sidecar/config.toml` holds a default configuration. This should work if installed on a Casper node.

If you install the Sidecar on an external server, you must update the `ip-address` values under `node_connections` appropriately.

### Node Connections

The Sidecar can connect to Casper nodes with versions greater or equal to `1.5.2`.

The `node_connections` option configures the node (or multiple nodes) to which the Sidecar will connect and the parameters under which it will operate with that node.

```
[[sse_server.connections]]
ip_address = "127.0.0.1"
sse_port = 9999
rest_port = 8888
max_attempts = 10
delay_between_retries_in_seconds = 5
allow_partial_connection = false
enable_logging = true
connection_timeout_in_seconds = 3
no_message_timeout_in_seconds = 60
sleep_between_keep_alive_checks_in_seconds = 30
```

* `ip_address` - The IP address of the node to monitor.
* `sse_port` - The node's event stream (SSE) port. This [example configuration](../resources/example_configs/EXAMPLE_NODE_CONFIG.toml) uses port `9999`.
* `rest_port` - The node's REST endpoint for status and metrics. This [example configuration](../resources/example_configs/EXAMPLE_NODE_CONFIG.toml) uses port `8888`.
* `max_attempts` - The maximum number of attempts the Sidecar will make to connect to the node. If set to `0`, the Sidecar will not attempt to connect.
* `delay_between_retries_in_seconds` - The delay between attempts to connect to the node.
* `allow_partial_connection` - Determining whether the sidecar will allow a partial connection to this node.
* `enable_logging` - This enables logging of events from the node in question.
* `connection_timeout_in_seconds` - Number of seconds before the connection request times out. Parameter is optional, defaults to 5
* `no_message_timeout_in_seconds` - Number of seconds after which the connection will be restarted if no bytes were received. Parameter is optional, defaults to 120
* `sleep_between_keep_alive_checks_in_seconds` - Optional parameter specifying the time intervals (in seconds) for checking if the connection is still alive. Defaults to 60

Connecting to multiple nodes requires multiple `[[sse_server.connections]]` sections:

```
[[sse_server.connections]]
ip_address = "127.0.0.1"
sse_port = 9999
rest_port = 8888
max_attempts = 10
delay_between_retries_in_seconds = 5
allow_partial_connection = false
enable_logging = true

[[sse_server.connections]]
ip_address = "18.154.79.193"
sse_port = 1234
rest_port = 3456
max_attempts = 10
delay_between_retries_in_seconds = 5
allow_partial_connection = false
enable_logging = true
```

### Storage

This directory stores the SSE cache and an SQLite database if the Sidecar is configured to use SQLite.

```
[storage]
storage_path = "/var/lib/casper-event-sidecar"
```

### Database Connectivity

<!--TODO for the Database Connectivity section, we could point to the Github README -->

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

### REST & Event Stream Criteria

This information determines outbound connection criteria for the Sidecar's `rest_server`.

<!--TODO for the REST & Event Stream Criteria section, we could point to the Github README -->

```
[rest_api_server]
port = 18888
max_concurrent_requests = 50
max_requests_per_second = 50
request_timeout_in_seconds = 10
```

* `port` - The port for accessing the Sidecar's `rest_server`. `18888` is the default, but operators are free to choose their own port as needed.
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

<!--TODO for the Admin Server section, we could point to the Github README -->

This optional section configures the Sidecar's administrative REST server. If this section is not specified, the Sidecar will not start an admin server.

```
[admin_api_server]
port = 18887
max_concurrent_requests = 1
max_requests_per_second = 1
```

* `port` - The port for accessing the Sidecar's admin REST server.
* `max_concurrent_requests` - The maximum total number of simultaneous requests that can be sent to the admin server.
* `max_requests_per_second` - The maximum total number of requests that can be sent per second to the admin server.

Access the admin server at `http://localhost:18887/metrics/`.

## Swagger Documentation

Once the Sidecar is running, access the Swagger documentation at `http://localhost:18888/swagger-ui/`.

## OpenAPI Specification

An OpenAPI schema is available at `http://localhost:18888/api-doc.json/`.

## Running the Event Sidecar

The `casper-event-sidecar` service starts after installation, using the systemd service file.

### Stop

`sudo systemctl stop casper-event-sidecar.service`

### Start

`sudo systemctl start casper-event-sidecar.service`

### Logs

`journalctl --no-pager -u casper-event-sidecar`