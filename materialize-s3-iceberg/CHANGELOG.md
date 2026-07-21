# Changelog

## 2026-07-20

### Added
- New advanced option `nanosecond_timestamps` materializes date-time fields as
  nanosecond-precision `timestamptz_ns` columns (Iceberg format v3) instead of
  microsecond-precision `timestamptz`. Toggling it on an existing
  materialization applies to data going forward: the timestamp columns are
  re-created under the new type (Iceberg has no in-place conversion), so
  existing rows read as null for those columns unless the binding is
  explicitly backfilled.
- With `nanosecond_timestamps` enabled, new tables are created as Iceberg
  format v3, and an existing format v2 table is upgraded to v3 automatically
  when its first nanosecond timestamp column is added.

### Fixed
- Parquet files now embed Iceberg field IDs. Previously, columns added by
  schema evolution could read as null in query engines that strictly apply the
  table's name mapping, because the mapping was never updated after table
  creation.
