# source-mysql

## 2026-08-18

### Added
- New `additional_backfill_filter` advanced option on each binding. When set,
  the filter clause is applied to all backfill queries for that table, so rows
  which the filter excludes are never backfilled. Setting or changing the
  filter requires re-backfilling the binding, while clearing it does not.
  Filters cannot be combined with the `Precise` backfill mode.

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

