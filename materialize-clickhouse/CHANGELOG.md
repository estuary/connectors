# Changelog

## 2026-08-25

### Added
- `1m`, `2m30s`, and `20m` are now valid `Sync Frequency` values, filling the
  gaps between `30s`-`5m` and `15m`-`30m`.

## 2026-08-11

### Fixed
- Materializing into an existing table whose `ORDER BY` does not cover the
  binding's key fields destroyed rows: ClickHouse combines rows that share a
  sorting key, so a table keyed on fewer columns than the collection discarded
  rows that were not duplicates. A table like this is now rejected when the
  materialization is published and when the task starts, naming the table's
  `ORDER BY`, the binding's key fields, and how to resolve it. To resolve it, drop
  or rename the table and backfill the binding, or point the binding at a
  different table; ClickHouse cannot change a table's sorting key in place. A
  sorting key that lists the same fields in a different order still works. One
  with extra fields also still works but is reported as a warning: no rows are
  lost, but the table keeps superseded versions of a key, so reads that do not use
  `FINAL` can return more than one row per key.
- A transaction that could not account for all of the rows it wrote failed after
  the fact and then committed anyway on the next restart, silently losing the rows
  that were missing. Such a transaction is now failed before it is committed and is
  retried in full.
- Recovering an interrupted transaction that is missing rows for no discoverable
  reason now fails with an error naming the table and both row counts, instead of
  committing what remains. Backfill the binding if its table is missing rows.
  This only affects transactions interrupted while running a version of the
  connector released before this one.

## 2026-08-10

### Fixed
- The load query joined the target table against the staging table of keys,
  which read every row of the target -- including the whole `flow_document`
  column -- for each transaction. On a large binding this could exceed the
  ClickHouse driver's read deadline, failing the query and restarting the
  transaction without it ever committing. The load query now restricts the
  target to the staged keys directly, so ClickHouse resolves them through the
  table's primary key index and reads only the rows being loaded.

## 2026-08-06

### Fixed
- Load waited for the prior transaction's acknowledgment before it could begin
  staging keys, which could force transactions to be smaller than necessary.
  Load now only waits for its persistent staging tables to be established, so
  staging can proceed concurrently with the prior transaction's acknowledgment.

### Changed
- Load batches are sent once a full batch is collected, instead of when the
  load binding changes.  On tasks with many active bindings, this should
  generally result in larger batch sizes.

## 2026-07-29

### Fixed
- Fixed a restart loop with the error `Table <database>.flow_temp_load_… does
  not exist` (code 60) on materializations with many bindings. The load phase
  began staging keys before the internal staging tables had finished being
  established, which happens concurrently, so a large materialization could
  reference a table that did not exist yet. Materializations using the
  `allow_existing_tables_for_new_bindings` or `retain_existing_data_on_backfill`
  feature flags were the most exposed, because those flags load every binding
  rather than skipping bindings known to be empty.
- Fixed a permanent restart loop with the error `refusing to load <table>: load
  table contains N keys but M were inserted` on ClickHouse Cloud. Truncating a
  replicated table — which on Cloud is every table, since `MergeTree` is
  transparently `SharedMergeTree` — happens asynchronously unless `SYNC` is
  requested, so a queued truncate of an internal staging table could execute
  after the rows of the next transaction had been staged and silently drop them.
  Truncating a table when backfilling a binding was exposed the same way, which
  could drop rows materialized just after the backfill was applied.

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
