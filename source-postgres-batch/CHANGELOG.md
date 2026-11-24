# source-postgres-batch

## 2026-11-24

### Changed
- A publish now fails when a binding's query template override uses cursor values but the binding has no cursor columns.

## 2026-11-23

### Added
- New `knownHosts` network tunnel option makes the connector verify the SSH server's host key.

### Changed
- Polling schedules no longer accept negative intervals.

## 2026-11-09

### Fixed
- The `verify-ca` and `verify-full` SSL modes now work with Amazon RDS, Aurora, and Google Cloud SQL instances that use the shared CA.
- The `verify-full` SSL mode now works through an SSH tunnel.

## 2026-11-02

### Changed
- Transactions containing sourced schemas are checkpointed immediately instead of after the connector's first poll. This may make task publications more responsive in certain setups.

## 2026-10-13

### Fixed
- `infinity` and `-infinity` values in date and timestamp columns are now captured as `9999-12-31T23:59:59Z` and `0000-01-01T00:00:00Z` instead of failing the capture.

## v1, 2023-07-31
- Beginning of changelog.
