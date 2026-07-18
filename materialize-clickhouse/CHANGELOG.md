# Changelog

## 2026-07-17

### Added
- New optional `partition_by` field on each table's resource configuration sets a custom [partitioning key](https://clickhouse.com/docs/engines/table-engines/mergetree-family/custom-partitioning-key) for the table, for example `toYYYYMM(flow_published_at)`. The expression is verified against your ClickHouse server when the materialization is published. Because ClickHouse only accepts a partitioning key at table creation, changing `partition_by` on an existing table requires backfilling the binding, which drops and re-creates the table.

### Fixed
- Fixed a permanent restart loop with the error `ALTER of key column ... is not safe because it can change the representation of primary key` (code 524) that occurred when a table's sorting-key or partition-key column was no longer part of the materialization's field selection. Such columns are now left unchanged instead of being made nullable, which ClickHouse forbids for key columns.

## 2026-07-15

### Fixed
- Fixed a permanent restart loop with the error `Tables have different structure` (code 122) that occurred when a table's schema was migrated while a transaction commit was pending. Recovery now updates the staging table to match the migrated table, with no data loss and no manual intervention required.
