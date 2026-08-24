# Changelog

All notable changes to this project will be documented in this file. The format is based on [Keep a Changelog].

[comment]: <> (Added: new features)
[comment]: <> (Changed: changes in existing functionality)
[comment]: <> (Deprecated: soon-to-be removed features)
[comment]: <> (Removed: now removed features)
[comment]: <> (Fixed: any bug fixes)
[comment]: <> (Security: in case of vulnerabilities)

## [Unreleased]

### Added

- Added JSON-RPC 2.0 batch and notification support to HTTP and WebSocket transports, including
  mixed `eth_subscribe` and `eth_unsubscribe` batches.
- Added configurable batch item and soft response-size limits with metrics for batch size and
  limit enforcement.
- Added an optional persistent, LMDB-backed cache (`rpc_server.binary_port_cache`) for immutable,
  identifier-addressed historical data (block headers, blocks with signatures, transactions with
  execution info) read over the node's binary port, surviving Sidecar restarts. Disabled by
  default.

### Changed

- Bumped `casper-json-rpc` to 3.0.0 with `JsonRpcOutput`, `JsonRpcOptions`, `Notification`, and the
  async `RequestDispatcher` API.
- JSON-RPC requests now accept fractional number IDs, distinguish missing IDs from explicit null
  IDs, and reject `params: null` in favor of omitted parameters or an empty array.

## [2.1.0]

### Added

- Bumped `casper-types` and `casper-binary-port` to be compatible with node 2.2.0 release.

## [2.0.0] -

### Added

- `account_put_transaction` now handles `TransactionInvocationTarget::ByPackageHash` with `protocol_version_major`
- `account_put_transaction` now handles `TransactionInvocationTarget::ByPackageName` with `protocol_version_major`
- Compatible with `casper-types` in 6.0.1 version

## [1.0.4] -

### Added

- Initial release of node for Sidecar.
