# Changelog

## 2026-07-15

### Added
- New advanced option `nanosecond_timestamps` materializes date-time fields as
  nanosecond-precision `timestamptz_ns` columns (Iceberg format v3) instead of
  microsecond-precision `timestamptz`. Enabling it on an existing
  materialization requires a backfill, because Iceberg has no in-place
  promotion between the two timestamp encodings.
- With `nanosecond_timestamps` enabled, new tables are created as Iceberg
  format v3, and an existing format v2 table is upgraded to v3 automatically
  when its first nanosecond timestamp column is added.
