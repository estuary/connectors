# materialize-sqlite

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
