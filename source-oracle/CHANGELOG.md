# Changelog

## 2026-09-29

### Changed
- An UPDATE which changes a row's primary key (or the capture's custom key
  columns) is now captured as a delete of the old row followed by an insert of
  the new row, instead of a single update at the new key. Destinations no longer
  keep an orphaned row under the old key. This requires the key columns to
  appear in LogMiner's undo SQL, which primary-key or all-column supplemental
  logging provides. When they are missing the change is captured as an update,
  as before.

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
- Captures no longer run discovery twice when they start up. Every restart
  previously issued two rounds of catalog queries in quick succession, which was
  most noticeable when many captures sharing a database all restarted at once.
- The timing of mid-capture rediscovery is now spread out rather than fixed, so
  captures which started at the same moment no longer query the catalog in
  lockstep every interval. The average rate of rediscovery is unchanged.
