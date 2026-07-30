# source-sqlserver-ct

## 2026-07-30

### Changed
- Backfill resume keys for `DATETIME2`, `SMALLDATETIME`, `DATETIMEOFFSET` and `DATE`
  primary-key columns are now bound as typed datetime parameters rather than as
  formatted strings. Comparing a datetime column against a string parameter left an
  implicit conversion inside the chunk query's resume predicate, which prevented SQL
  Server from using an index seek on tables whose primary key has more than one
  column. Each chunk instead rescanned every row it had already read, so backfills of
  large tables progressed more and more slowly as they went on.

## 2026-07-22

### Added
- New "Discovery Filters" configuration section which controls the set of
  tables visible to discovery. Where these filters overlap with existing
  Advanced Options, the settings are combined as a union.

### Changed
- Discovery fetches per-table metadata in chunks, so discovery of databases with
  very large numbers of tables no longer times out or exhausts memory.

## 2026-07-22

### Changed

- Change polling now uses a single batched query to identify tables with new changes prior
  to reading out those changes. This improves efficiency when capturing mostly-idle tables.
