# Changelog

All notable changes to this project will be documented in this file. The format is based on [Keep a Changelog].

[comment]: <> (Added: new features)
[comment]: <> (Changed: changes in existing functionality)
[comment]: <> (Deprecated: soon-to-be removed features)
[comment]: <> (Removed: now removed features)
[comment]: <> (Fixed: any bug fixes)
[comment]: <> (Security: in case of vulnerabilities)

## [2.1.0] -

### Added

- JSON RPC API has a new action "state_query_bids".

## [2.0.0] -

### Added

- `account_put_transaction` now handles `TransactionInvocationTarget::ByPackageHash` with `protocol_version_major`
- `account_put_transaction` now handles `TransactionInvocationTarget::ByPackageName` with `protocol_version_major`
- Compatible with `casper-types` in 6.0.1 version

## [1.0.4] -

### Added

- Initial release of node for Sidecar.
