# Changelog

## 2026-07-22

### Added
- New advanced option `variant_columns` materializes object, array, and
  multi-type fields — and the root document — as Iceberg `variant` columns
  instead of JSON strings, and creates new tables with Iceberg format v3.
  Collection key fields keep their string mapping, and the per-field
  `castToString` option still forces a JSON string column. Reading variant
  columns requires an Iceberg v3-capable query engine (for example Spark 4.0,
  Snowflake, or DuckDB 1.5.3 and later).
- Enabling `variant_columns` on an existing materialization converts its
  tables in place: each table upgrades to Iceberg format v3 and every affected
  column is re-created under the same name as a variant column (Iceberg has no
  in-place conversion), applying to data going forward. Existing rows read as
  null for converted columns unless the binding is explicitly backfilled;
  prior snapshots remain readable in full via time travel. Key fields and
  `castToString` fields are untouched.

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
