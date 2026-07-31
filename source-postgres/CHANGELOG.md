# Changelog

## 2026-07-31

### Fixed
- Captures with `read_only_capture` enabled no longer stall when the source
  database writes WAL for tables which aren't being captured.

## 2026-07-30

### Added
- New `rediscovery_interval` advanced option controls how often the connector
  re-runs discovery while a capture is running, to notice schema changes and
  newly added tables. It defaults to 15 minutes.

### Changed
- Captures no longer run discovery twice when they start up.
- The timing of mid-capture rediscovery is now spread out rather than fixed, so
  captures which started at the same moment no longer query the catalog in
  lockstep every interval. The average rate of rediscovery is unchanged.

## 2026-07-21

### Added
- New "Discovery Filters" configuration section which controls the set of
  tables visible to discovery. Where these filters overlap with existing
  Advanced Options, the settings are combined as a union.

### Changed
- Discovery fetches per-table metadata in chunks, so discovery of databases with
  very large numbers of tables no longer times out or exhausts memory.

## 2026-07-20

### Fixed
- Replication slot creation and deletion now use replication protocol commands
  instead of SQL functions. This avoids holding a transaction snapshot while
  slot creation blocks waiting for a consistent point.

## 2026-07-16

### Changed
- Replication slot creation is exempted from `statement_timeout`, slot creation
  blocks indefinitely rather than timing out in the presence of long-running
  transactions.

## 2026-07-14

### Changed
- Queries now use a default `statement_timeout` of 2 minutes when the
  `statement_timeout` advanced option is left blank. Set to `0` for the
  previous behavior of disabling timeouts.
