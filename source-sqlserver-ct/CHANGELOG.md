# source-sqlserver-ct

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
