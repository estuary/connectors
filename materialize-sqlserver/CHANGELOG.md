# materialize-sqlserver

## 2026-10-22

### Added
- The connector status on the task page now shows what the materialization is
  doing, such as loading, staging or committing data, or waiting for its next
  scheduled sync.

### Changed
- The warning about rows a backfill did not delete now names every affected
  table instead of only the latest one.

## 2026-10-12

### Fixed
- A connection or login failure is now reported as that, instead of as a
  collation check error. This also applies when the materialization is saved,
  so messages such as "host cannot be found" are shown again.

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
