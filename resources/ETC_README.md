# Casper Sidecar README for Node Operators

This page contains specific instructions for node operators. Before proceeding, familiarize yourself with the main [README](../README.md) file, which covers the following:
 - [Summary of purpose](../README.md#summary-of-purpose)
 - [System components and architecture](../README.md#system-components-and-architecture)
 - [Configuration options](../README.md#configuring-the-sidecar)
 - [Running and testing the Sidecar](../README.md#running-and-testing-the-sidecar)
 - [Troubleshooting tips](../README.md#troubleshooting-tips)

## Sidecar Configuration on the Node

The file `/etc/casper-sidecar/config.toml` holds a default configuration. This should work if installed on a Casper node.

If you install the Sidecar on an external server, you must update the `ip-address` values under `node_connections` appropriately.

For more information, including how to setup the SSE, RPC, REST, and Admin servers, read the [configuration options](../README.md#configuring-the-sidecar) in the main README.

## Storage on the Node

This directory stores the SSE cache and a database if the Sidecar was configured to use one.

```toml
[storage]
storage_path = "/var/lib/casper-sidecar"
```

The DB setup is described [here](../README#database-connectivity-setup).

## Running the Sidecar on a Node

The `casper-sidecar` service starts after installation, using the systemd service file.

### Stop

`sudo systemctl stop casper-sidecar.service`

### Start

`sudo systemctl start casper-sidecar.service`

### Logs

`journalctl --no-pager -u casper-sidecar`

## Swagger Documentation

If the Sidecar is running locally, access the Swagger documentation at `http://localhost:18888/swagger-ui/`.

## OpenAPI Specification

An OpenAPI schema is available at `http://localhost:18888/api-doc.json/`.