# Changelog

## 2026-09-23

### Fixed
- Empty string values were written to string columns as the two-character
  string `""`. They are now written as empty strings. Null values are
  unaffected.

## 2026-09-13

### Added
- Objects, arrays, multi-type fields, and the root document can be stored as
  Iceberg format v3 `variant` columns instead of JSON strings, so they can be
  queried natively without `parse_json`. This is controlled by the
  `variant_columns` feature flag. Tables with a variant column are created as
  format v3, and an existing v2 table is upgraded to v3 when its first variant
  column is added. Collection keys stay string columns, `castToString` still
  forces a JSON string column, and string-encoded numbers keep their numeric
  columns. Values inside a variant keep their JSON types: strings stay
  strings, including those with a `format` annotation.
- Variant columns need the EMR Serverless application to run Spark 4 (release
  `emr-spark-8.0.0` or later); this is checked when the materialization is
  published. Set the `no_variant_columns` feature flag to keep JSON strings on
  an older application or a catalog without format v3 support.

### Changed
- Turning `variant_columns` on or off for an existing materialization converts
  the affected columns in place, in either direction, and keeps the existing
  rows; unlike materialize-s3-iceberg, no backfill is needed. When a column
  that held plain strings is converted to variant, a value that is itself valid
  JSON (such as `42` or `true`) becomes that JSON type. Existing
  materializations keep their JSON string columns until the flag is set.

## 2026-09-09

### Added
- Add support for overriding Spark executor defaults for jobs started by
  EMR.

## 2026-09-03

### Changed
- Use a client idempotency token to avoid duplicate merge jobs caused by
  connector restarts when possible.

## 2026-08-28

### Changed
- The commit schedule is now offset per materialization rather than per EMR
  application. Previously every materialization sharing an EMR Serverless
  application committed at the same instant, so its `maximumCapacity` had to
  cover the resulting spike rather than the average. Tasks are now spread across
  the sync interval. Commit cadence is unchanged; each task shifts to its own
  offset once, on upgrade.

### Added
- Add 4 hour limit to EMR job run time after which the job will be cancelled.

## 2026-08-25

### Added
- `1m`, `2m30s`, and `20m` are now valid `Sync Frequency` values, filling the
  gaps between `30s`-`5m` and `15m`-`30m`.

## 2026-07-29

### Added
- New advanced option `table_identifier_case` controls the casing of namespace
  and table names created by the materialization. It accepts `lowercase` (the
  default, which folds names to lower case), `uppercase` (folds names to upper
  case, needed for case-sensitive catalogs such as Snowflake's where unquoted
  identifiers resolve upper-case), and `preserve` (keeps names as written in the
  spec). This replaces the never-released `all_caps_table_names` and
  `all_caps_identifiers` options.
- New advanced option `field_name_case` controls the casing of column names
  created by the materialization, using the same values as
  `table_identifier_case`: `preserve` (the default, which names columns exactly
  as the collection names its fields), `lowercase`, and `uppercase`. Columns that
  already exist are matched case-insensitively and are never renamed, so this
  option only affects columns as they are created.

### Deprecated
- The advanced option `lowercase_column_names` is deprecated in favor of
  `field_name_case: lowercase`, which it remains equivalent to. Setting it
  alongside a `field_name_case` of `uppercase` or `preserve` is an error. It is
  no longer offered in the connector's configuration schema, but materializations
  that already set it continue to work.

### Fixed
- The namespace pre-created at apply time now always matches the namespace that
  tables are created in. Previously, a configured namespace containing upper
  case letters or special characters would additionally create an unused
  namespace with the verbatim configured name.
