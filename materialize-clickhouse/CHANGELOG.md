# Changelog

## 2026-07-17

### Fixed
- Replaced a cryptic `No such column _is_deleted` (code 16) crash loop with a clear, actionable error when hard delete is enabled but a standard-updates table is missing the connector's internal `_is_deleted` column (for example, a pre-created table adopted by the materialization, or a table created before hard delete was enabled). The error explains that the table must be recreated by backfilling the binding, or that hard delete should be disabled.
- Fixed a permanent restart loop with the error `ALTER of key column ... is not safe because it can change the representation of primary key` (code 524) that occurred when a table's sorting-key or partition-key column was no longer part of the materialization's field selection. Such columns are now left unchanged instead of being made nullable, which ClickHouse forbids for key columns.

## 2026-07-15

### Fixed
- Fixed a permanent restart loop with the error `Tables have different structure` (code 122) that occurred when a table's schema was migrated while a transaction commit was pending. Recovery now updates the staging table to match the migrated table, with no data loss and no manual intervention required.
