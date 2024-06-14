# Casper Sidecar README for Node Operators

This page contains specific instructions for node operators. Before proceeding, familiarize yourself with the main [README](../README.md) file, which covers the following:
 - [Summary of purpose](../README.md#summary-of-purpose)
 - [System components and architecture](../README.md#system-components-and-architecture)
 - [Configuration options](../README.md#configuring-the-sidecar)
 - [Running and testing the Sidecar](../README.md#running-and-testing-the-sidecar)
 - [Troubleshooting tips](../README.md#troubleshooting-tips)


## Configuring the Sidecar

The file `/etc/casper-sidecar/config.toml` holds a default configuration. This should work if installed on a Casper node.

If you install the Sidecar on an external server, you must update the `ip-address` values under `node_connections` appropriately.

For more information, including how to setup the SSE, RPC, REST, and Admin servers, read the [configuration options](../README.md#configuring-the-sidecar) in the main README.


## Installing the Sidecar on a Node

The following command will install the Debian package for the Casper Sidecar service on various flavors of Linux. 

<!-- TODO Once the package is published, update the command below with the new link to the *.deb package. The link below assumes a package available locally. -->

```bash
sudo apt install ./casper-sidecar_0.1.0-0_amd64.deb
```

Check the service status:

```bash
systemctl status casper-sidecar
```

Check the logs and make sure the service is running as expected.

```bash
journalctl --no-pager -u casper-sidecar
```

If you see any errors, you may need to [update the configuration](#configuring-the-service) and restart the service with the commands below.

## Running the Sidecar on a Node

The `casper-sidecar` service starts after installation, using the systemd service file.

### Stop

`sudo systemctl stop casper-sidecar.service`

### Start

`sudo systemctl start casper-sidecar.service`


## Sidecar Storage

This directory stores the SSE cache and a database if the Sidecar was configured to use one.

```toml
[storage]
storage_path = "/var/lib/casper-sidecar"
```

The DB setup is described [here](../README#database-connectivity-setup).

## Swagger Documentation

If the Sidecar is running locally, access the Swagger documentation at `http://localhost:18888/swagger-ui/`.

## OpenAPI Specification

An OpenAPI schema is available at `http://localhost:18888/api-doc.json/`.