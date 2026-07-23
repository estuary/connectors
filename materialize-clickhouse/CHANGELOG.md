# Changelog

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

## 2026-07-22

### Changed
- Errors that occur while loading or storing data now name the affected table, for example ``flushing store batch for `db`.`my_table`: code: 252, ...``. Previously these errors did not identify which table caused them, which made diagnosis difficult on materializations with many bindings.

## 2026-07-17

### Added
- New optional `partition_by` field on each table's resource configuration sets a custom [partitioning key](https://clickhouse.com/docs/engines/table-engines/mergetree-family/custom-partitioning-key) for the table, for example `toYYYYMM(flow_published_at)`. The expression is verified against your ClickHouse server when the materialization is published. Because ClickHouse only accepts a partitioning key at table creation, changing `partition_by` on an existing table requires backfilling the binding, which drops and re-creates the table.

### Fixed
- Replaced a cryptic `No such column _is_deleted` (code 16) crash loop with a clear, actionable error when hard delete is enabled but a standard-updates table is missing the connector's internal `_is_deleted` column (for example, a pre-created table adopted by the materialization, or a table created before hard delete was enabled). The error explains that the table must be recreated by backfilling the binding, or that hard delete should be disabled.
- Fixed a permanent restart loop with the error `ALTER of key column ... is not safe because it can change the representation of primary key` (code 524) that occurred when a table's sorting-key or partition-key column was no longer part of the materialization's field selection. Such columns are now left unchanged instead of being made nullable, which ClickHouse forbids for key columns.

## 2026-07-15

### Fixed
- Fixed a permanent restart loop with the error `Tables have different structure` (code 122) that occurred when a table's schema was migrated while a transaction commit was pending. Recovery now updates the staging table to match the migrated table, with no data loss and no manual intervention required.
