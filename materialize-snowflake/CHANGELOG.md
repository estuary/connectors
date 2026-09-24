# materialize-snowflake

## 2026-09-24

### Fixed
- Reduced memory used while uploading staged files, which since the
  2026-09-22 driver update could exceed the connector's memory limit and
  restart the task repeatedly when several large files for one table were
  uploaded at once.

## 2026-09-21

### Fixed
- When syncing automatic clustering, the connector now checks the clustering
  state of exactly the target table. Previously, a table in the same schema
  whose name differed only where the target's name has an underscore could be
  read instead, causing a clustering key to be dropped or kept incorrectly.

## 2026-09-12

### Fixed
- Support loading the `flow_document` from non-variant columns.  This can be
  used along with a custom DDL and castToString to store the flow_document as a
  string, which may be required if the document contains certain values.

## 2026-09-11

### Fixed
- Fix channel closed panic when debug logging is enabled.

## 2026-08-25

### Added
- `1m`, `2m30s`, and `20m` are now valid `Sync Frequency` values, filling the
  gaps between `30s`-`5m` and `15m`-`30m`.

## 2026-08-24

### Fixed
- A `number` column no longer fails the transaction when it receives a whole
  number larger than the `int64` maximum. These values arrive as `uint64` or
  `big.Int`, and the connector now converts them to a float.

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

## v1, 2022-07-27
- Beginning of changelog.
