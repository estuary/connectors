# materialize-bigquery

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

## 2026-07-18

### Fixed
- Fixed a permanent `403 Access Denied: ... does not have permission to access policy
  tag ... on column flow_temp_table_N.cN` error when materializing to tables that use
  BigQuery column-level security (policy tags). The connector's internal staging table
  no longer inherits policy tags from destination columns; BigQuery enforced access
  control on that staging table regardless of the service account's Fine-Grained
  Reader grants, so no permissioning change could resolve the error.

## v1, 2022-07-27
- Beginning of changelog.
