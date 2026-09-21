# Changelog

## 2026-08-25

### Added
- `1m`, `2m30s`, and `20m` are now valid `Sync Frequency` values, filling the
  gaps between `30s`-`5m` and `15m`-`30m`.

## 2026-08-17

### Added
- New advanced option `variant_columns` materializes object, array, and
  multi-type fields — and the root document — as Iceberg `variant` columns
  instead of JSON strings, and creates new tables with Iceberg format v3.
  Collection key fields keep their string mapping, the per-field
  `castToString` option still forces a JSON string column, and string-encoded
  numbers (`string` plus `integer`/`number` with a matching `format`
  annotation) keep their numeric column. Reading variant
  columns requires an Iceberg v3-capable query engine (for example Spark 4.0,
  Snowflake, or DuckDB 1.5.3 and later).
- Strings in a variant column are stored with the type implied by their format
  annotation, matching how the same value is typed as a column of its own:
  `{format: date-time}` is stored as a variant timestamp (nanosecond precision
  when `nanosecond_timestamps` is also enabled), `{format: date}` as a variant
  date, and `{contentEncoding: base64}` as variant binary. Formats with no
  variant equivalent — `uuid`, `time`, and `duration` — are stored as strings,
  as they are in a column of their own, and a value that fails to parse as its
  annotated type is stored as a string. Only top-level fields carry format
  annotations, so values nested inside an object, an array, or the root
  document are stored as strings.
- Enabling `variant_columns` on an existing materialization converts its
  tables in place: each table upgrades to Iceberg format v3 and every affected
  column is re-created under the same name as a variant column (Iceberg has no
  in-place conversion), applying to data going forward. Existing rows read as
  null for converted columns unless the binding is explicitly backfilled;
  prior snapshots remain readable in full via time travel. Key fields and
  `castToString` fields are untouched.

## 2026-07-23

### Changed
- Schema changes now first commit any transaction that was staged but not yet
  fully applied to the tables they affect, instead of leaving it to be applied
  afterwards. This prevents failures where staged data built against the
  previous table schema could no longer be applied after a column was added,
  made nullable, or had its type migrated.

### Fixed
- With the `retain_existing_data_on_backfill` feature flag enabled, backfilling
  a binding no longer risks losing the rows of a transaction that was committed
  but not yet fully applied to the destination: the pending transaction is now
  applied before the backfill takes effect.

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
