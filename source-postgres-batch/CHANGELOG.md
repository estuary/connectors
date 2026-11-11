# source-postgres-batch

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
