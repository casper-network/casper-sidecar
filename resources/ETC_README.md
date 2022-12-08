## Summary of Purpose

The Casper Event Sidecar is an application that runs in tandem with the node process. This reduces the load on the node process by allowing subscribers to monitor the event stream through the Sidecar, while the node focuses entirely on the blockchain. Users needing access to the JSON-RPC will still need to query the node directly.

While the primary use case for the Sidecar application is running alongside the node on the same machine, it can be run remotely if necessary.

### System Components & Architecture

Casper Nodes offer a Node Event Stream API returning Server-Sent Events (SSEs) that hold JSON-encoded data. The SSE Sidecar uses this API to achieve the following goals:

* Build a sidecar middleware service that connects to the Node Event Stream, with a passthrough that replicates the SSE interface of the node and its filters (i.e., `/main`, `/deploys`, and `/sigs` with support for the use of the `?start_from=` query to allow clients to get previously sent events from the Sidecar's buffer).

* Provide a new RESTful endpoint that is discoverable to node operators.

The SSE Sidecar uses one ring buffer for outbound events, providing some robustness against unintended subscriber disconnects. If a disconnected subscriber re-subscribes before the buffer moves past their last received event, there will be no gap in the event history if they use the `start_from` URL query.


## Configuration

The file `/etc/casper-event-sidecar/config.toml` holds a default configuration. This should work if installed on a Casper node.

If you install the Sidecar on an external server, you must update the `ip-address` values under `node_connections` appropriately.

### Node Connections

```rust
node_connections = [
    {  ip_address = "127.0.0.1", sse_port = 18101, max_retries = 5, delay_between_retries = 5, enable_event_logging = true  },
    {  ip_address = "127.0.0.1", sse_port = 18102, max_retries = 5, delay_between_retries = 5, enable_event_logging = false  },
]
```

The `node_connections` option configures the node (or multiple nodes) to which the Sidecar will connect and the parameters under which it will operate with that node.

* `ip_address` - The IP address of the node to monitor.
* `sse_port` - The node's event stream (SSE) port, `9999` by default.
* `max_retries` - The maximum number of attempts the Sidecar will make to connect to the node. If set to `0`, the sidecar will not attempt to reconnect.
* `delay_between_retries_in_seconds` - The delay between attempts to connect to the node.
* `allow_partial_connection` - Determing whether the sidecar will allow a partial connection to this node.
* `enable_event_logging` - This enables logging of events from the node in question.

### Storage

```
[storage]
storage_path = "/var/lib/casper-event-stream"
```
This directory stores the SQLite database for the Sidecar and the SSE cache.

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

### REST & Event Stream Criteria

```
[rest_server]
port = 18888
max_concurrent_requests = 50
max_requests_per_second = 50
request_timeout_in_seconds = 10
```

This information determines outbound connection criteria for the Sidecar's `rest_server`.

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

## Running the Event Sidecar

The `casper-event-sidecar` service starts after installation, using the systemd service file.

### Stop

`sudo systemctl stop casper-event-sidecar.service`

### Start

`sudo systemctl start casper-event-sidecar.service`

### Logs

`journalctl --no-pager -u casper-event-sidecar`