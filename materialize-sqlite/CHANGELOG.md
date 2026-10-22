# materialize-sqlite

## 2026-10-22

### Added
- The connector status on the task page now shows what the materialization is doing, such as loading, staging or committing data, or waiting for its next scheduled sync.

### Changed
- The warning about rows a backfill did not delete now names every affected table instead of only the latest one.

## 2026-10-01

### Added
- When a collection is backfilled, rows the backfill did not re-send are deleted
  once it completes, so documents removed from the source no longer linger in
  the destination. A table that excludes the `flow_published_at` field is left
  unchanged, and a warning on the task page explains why.
  Disable it with the `no_truncate_after_backfill` feature flag. It is also
  disabled while `retain_existing_data_on_backfill` is enabled.

### Fixed
- Updating an existing row no longer fails with a syntax error when the table has no columns besides its keys and `flow_document`.
- Restarting the connector against an existing database file no longer fails with `table flow_temp_table_<n> already exists`.

## v1, 2023-03-10

- Beginning of changelog.
