# materialize-bigquery

## 2026-09-17

### Added
- New optional `partition_by` field on each table's resource configuration sets
  the table's
  [partitioning](https://cloud.google.com/bigquery/docs/partitioned-tables), for
  example `DATE(created_at)`, `TIMESTAMP_TRUNC(updated_at, MONTH)`,
  `_PARTITIONDATE` or `RANGE_BUCKET(id, GENERATE_ARRAY(0, 1000, 10))`. The
  expression is verified against BigQuery when the materialization is published,
  and is applied every time the connector creates the table, so partitioning now
  survives backfills that drop and re-create it. Because BigQuery only accepts
  partitioning at table creation, changing `partition_by` on an existing table
  requires backfilling the binding, which drops and re-creates the table. An
  existing table whose partitioning does not match `partition_by` is rejected at
  publish, and a partitioned table with no `partition_by` set logs a warning
  suggesting the expression to record.

## 2026-09-14

### Fixed
- A transaction whose load query returns more than 10 GB of documents no longer
  fails with `responseTooLarge`. Load results are now written to a
  per-transaction table in the endpoint dataset, named
  `flow_load_results_<materialization>_<range>_<uuid>`, which is deleted after
  read-back and expires after one day if a crash prevents the deletion.

### Changed
- Load results were previously held in an anonymous table in the billing
  project. They are now briefly stored in the endpoint dataset of the
  configured project, so that storage is attributed there.

## 2026-08-31

### Added
- Support for tasks scaled out to multiple shards.

## 2026-08-25

### Added
- `1m`, `2m30s`, and `20m` are now valid `Sync Frequency` values, filling the
  gaps between `30s`-`5m` and `15m`-`30m`.

## 2026-08-07

### Fixed
- A load query that reads a NULL `flow_document` now says so, and names the
  `Exclude Flow Document` option that addresses it. It previously reported
  `value[1] wrong type int64 expecting string`, giving the type of the binding
  index rather than of the document, which made it the same message regardless
  of cause.

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
