# casper-event-sidecar

### Summary of Purpose

The sidecar application will run on the same host as the node process.
It will consume the node's event stream with the aim of providing:
- A stable external interface for clients who would have previously connected directly to the node process.
- Access to historic data via convenient interfaces.
- The ability to query for filtered data.

### Running the Sidecar
After cloning the code to your local machine you will need to tweak the Config (`/src/config.toml`).
Set network to "Local":
```toml
[connection]
network = "Local"
```
and then ensure that the "Local" node settings are correct for your instance:
```toml
[connection.node.local]
ip_address="0.0.0.0"
sse_port=9999
```
You can also change the port on which the sidecar's Event Stream will be exposed:
```toml
[sse_server]
port = 19999
```

Once you are happy with the configuration you can (build and) run it using Cargo:
```shell
cargo run
```
