# source-sqlserver

## 2026-07-23

### Changed
- Discovery queries now always reference the `INFORMATION_SCHEMA` catalog using
  its canonical uppercase names. This matches the stored catalog names in every
  collation, so discovery works under case-mapping collations such as
  Turkish/Azeri without any additional configuration. The internal
  `uppercase_discovery_queries` feature flag becomes a no-op.

## 2026-07-22

### Added
- New "Discovery Filters" configuration section which controls the set of
  tables visible to discovery. Where these filters overlap with existing
  Advanced Options, the settings are combined as a union.

### Changed
- Discovery fetches per-table metadata in chunks, so discovery of databases with
  very large numbers of tables no longer times out or exhausts memory.
