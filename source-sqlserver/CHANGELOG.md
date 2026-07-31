# source-sqlserver

## 2026-07-31

### Fixed
- The startup check for whether CDC is enabled now binds the database name as a
  query parameter instead of splicing it into the statement, so every capture
  issues the same query rather than one plan's worth of work per database name.
- Automatic change table cleanup now binds its watermark as a query parameter
  instead of formatting it into the statement. Every cleanup call previously
  produced a statement SQL Server had never seen before, so it compiled a plan
  for each one, used it once and discarded it. This only affected captures with
  `change_table_cleanup` enabled.
- The polling query which checks each CDC capture instance for new changes now
  lists those instances in a stable order. It was previously assembled in map
  iteration order, so its text differed on every polling cycle and SQL Server
  compiled a new plan for it each time instead of reusing a cached one.
- Replication diagnostics are no longer collected on every streaming cycle when
  a large `polling_interval` is configured. A streaming cycle cannot finish any
  faster than one polling interval, so the fixed five minute threshold for
  treating a cycle as unexpectedly long was exceeded on every cycle, and each
  capture then repeatedly ran a set of queries against server-wide metadata.
  The threshold is now that five minute grace period on top of the configured
  polling interval.

## 2026-07-30

### Added
- New `rediscovery_interval` advanced option controls how often the connector
  re-runs discovery while a capture is running, to notice schema changes and
  newly added tables. It defaults to 15 minutes.

### Fixed
- Discovery queries are now shaped so that SQL Server reuses a single cached
  query plan across repeated discovery passes, rather than compiling a new plan
  for every chunk of tables every time. On instances hosting many databases the
  previous behavior could exhaust query compile memory and drive the server to
  sustained high CPU, blocking unrelated queries.

### Changed
- Backfill resume keys for `DATETIME2`, `SMALLDATETIME`, `DATETIMEOFFSET` and `DATE`
  primary-key columns are now bound as typed datetime parameters rather than as
  formatted strings. Comparing a datetime column against a string parameter left an
  implicit conversion inside the chunk query's resume predicate, which prevented SQL
  Server from using an index seek on tables whose primary key has more than one
  column. Each chunk instead rescanned every row it had already read, so backfills of
  large tables progressed more and more slowly as they went on.
- Captures no longer run discovery twice when they start up.
- The timing of mid-capture rediscovery is now spread out rather than fixed, so
  captures which started at the same moment no longer query the catalog in
  lockstep every interval. The average rate of rediscovery is unchanged.

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
