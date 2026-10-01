# materialize-sqlserver

## 2026-10-01

### Added
- When a collection is backfilled, rows the backfill did not re-send are deleted
  from standard-updates tables once it completes, so documents removed from the
  source no longer linger in the destination. Delta-updates tables keep every
  row. A table that excludes the `flow_published_at` field is left unchanged,
  and a warning on the task page explains why.
  Disable it with the `no_truncate_after_backfill` feature flag. It is also
  disabled while `retain_existing_data_on_backfill` is enabled.

## v1, 2023-09-01
- Beginning of changelog.
