# Casper Event Sidecar README

## Summary of Purpose

The Casper Event Sidecar is an application that runs on the same host as the node process. This offloads querying processes, allowing the node to focus entirely on the blockchain.

While the primary use case for the Sidecar application is running alongside the node on the same machine, it can be run remotely if necessary.

It will consume the node's event stream with the aim of providing:

- A stable external interface for clients who would have previously connected directly to the node process.

- Access to historic data via convenient interfaces.

- The ability to query for filtered data.

![Sidecar Diagram](https://user-images.githubusercontent.com/102557486/194373652-042e112b-bec8-4557-a28f-98c30a179e0c.png)

## Prerequisites

* CMake 3.1.4 or greater
* [Rust](https://www.rust-lang.org/tools/install)
* pkg-config
* gcc
* g++

## Setting Up *Config.toml*

The file *config.toml* in the base *event-sidecar* directory contains configuration details for your instance of the Sidecar application. These must be adjusted prior to running the application.


### Node Connections

```rust
node_connections = [
    {  ip_address = "127.0.0.1", sse_port = 18101, max_retries = 5, delay_between_retries = 5, enable_event_logging = true  },
    {  ip_address = "127.0.0.1", sse_port = 18102, max_retries = 5, delay_between_retries = 5, enable_event_logging = false  },
]
```

The `node_connections` option configures the node (or multiple nodes) to which the Sidecar will connect and the parameters under which it will operate with that node.

* `ip_address` - The IP address of the note to monitor.
* `sse_port` - The node's port.
* `max_retries` - The maximum number of attempts the Sidecar will make to connect to the node.
* `delay_between_retries` - The delay between attempts to connect to the node.
* `enable_event_logging` - This enables logging of events from the node in question.

### Storage

```
[storage]
storage_path = "./target/storage"
```
The directory that stores the sqlite database for the Sidecar and the SSE cache.

### Sqlite Database

```
[storage.sqlite_config]
file_name = "sqlite_database.db3"
max_write_connections = 10
max_read_connections = 100
# https://www.sqlite.org/compile.html#default_wal_autocheckpoint
wal_autocheckpointing_interval = 1000
```

This section includes configurations for the `sqlite` database.

* `file_name` - The database file path.
* `max_write_connections` - The maximum number of connections with `write` access to the database. (Should generally be left as is.)
* `max_read_connections` - The maximum number of connections with `read` access to the database. (Should generally be left as is.)
* `wal_autocheckpointing_interval` - This controls how often the system commits pages to the database. The value determines the maximum number of pages before forcing a commit. More information can be found [here](https://www.sqlite.org/compile.html#default_wal_autocheckpoint).

### Rest & Event Stream Criteria

```
[rest_server]
ip_address = "127.0.0.1"
port = 17777
```

This information determines outbound connection criteria for the Sidecar's `rest_server`. Our suggested syntax for the port is to use the same port as the associated node, but prefixed with a `1`. Therefore, if the port is running on port `7777`, the Sidecar would use port `1777`.

```
[event_stream_server]
ip_address = "127.0.0.1"
port = 19999
max_concurrent_subscribers = 100
event_stream_buffer_length = 5000
```

The `event_stream_server` section specifies the IP address and port for the Sidecar's event stream. As above, we suggest that the port be the same as the node's event stream port, prefixed with a `1`.

Additionally, there are the following two options:

* `max_concurrent_subscribers` - The maximum number of subscribers that can monitor the Sidecar's event stream.
* `event_stream_buffer_length` - The number of events that the stream will hold in its buffer for reference when a subscriber reconnects.

## Unit Testing the Sidecar Application

You can run included unit and integration tests with the following command:

```
cargo test
```

## Running the Sidecar

Once you are happy with the configuration you can (build and) run it using Cargo:

```shell
cargo run
```
Or you can run the Sidecar with the following options:

The least verbose level, logging `Info` events:

```
RUST_LOG=info cargo run -p casper-event-sidecar -- -p "EXAMPLE_CONFIG.toml"
```

Logging both `Info` and `Debug` events

```
RUST_LOG=debug cargo run -p casper-event-sidecar -- -p "EXAMPLE_CONFIG.toml"
```

The most verbose level, logging `Trace` events in addition to `Debg` and `Info`.

```
RUST_LOG=trace cargo run -p casper-event-sidecar -- -p "EXAMPLE_CONFIG.toml"
```

## Testing Sidecar with a Local Network using NCTL

Your instance of the Sidecar application can be tested against a local network by using NCTL.

Instructions for setting up NCTL can be found [here](https://docs.casperlabs.io/dapp-dev-guide/building-dapps/setup-nctl/).

The configuration shown within this README will direct the Sidecar application to a locally hosted NCTL network, if one is running. The Sidecar should function in the same way that it would with a live node, displaying events as they occur in the local NCTL network.
