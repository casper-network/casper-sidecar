# rpc-sidecar

[![LOGO](https://raw.githubusercontent.com/casper-network/casper-node/master/images/casper-association-logo-primary.svg)](https://casper.network/)

[![Build Status](https://drone-auto-casper-network.casperlabs.io/api/badges/casper-network/casper-node/status.svg?branch=dev)](http://drone-auto-casper-network.casperlabs.io/casper-network/casper-node)
[![Crates.io](https://img.shields.io/crates/v/casper-rpc-sidecar)](https://crates.io/crates/casper-rpc-sidecar)
[![License](https://img.shields.io/badge/license-Apache-blue)](https://github.com/CasperLabs/casper-node/blob/master/LICENSE)

## Synopsis

The Casper Sidecar provides connectivity to the binary port of a Casper node (among [other capabilities](../README.md#system-components-and-architecture)), exposing a JSON-RPC interface for interacting with that node. The RPC protocol allows for basic operations like querying global state, sending transactions and deploys, etc. All of the available RPC methods are documented [here](https://docs.casper.network/developers/json-rpc/).

## Protocol

The Sidecar maintains a TCP connection with the node and communicates using a custom binary protocol, which uses a request-response model. The Sidecar sends simple self-contained requests and the node responds to them. The requests can be split into these main categories:
- Read requests
    - Queries for transient in-memory information like the current block height, peer list, component status etc.
    - Queries for database items, with both the database and the key always being explicitly specified by the sidecar
- Transaction requests
    - Requests to submit transactions for execution
    - Requests to speculatively execute a transactions 

## Discovering the JSON RPC API

Once setup and running as described [here](../README.md), the Sidecar can be queried for its JSON RPC API using the `rpc.discover` method, as shown below. The result will be a list of RPC methods and their parameters.

```bash
curl -X POST http://localhost:<RPC_SERVER_PORT>/rpc -H 'Content-Type: application/json' -d '{"jsonrpc": "2.0", "method": "rpc.discover", "id": 1}'
```

## License

Licensed under the [Apache License Version 2.0](https://github.com/casper-network/casper-node/blob/master/LICENSE).
