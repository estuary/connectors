# source-bigquery-batch

## 2026-09-22

### Fixed
- Discovery no longer fails with `panic: interface conversion: bigquery.Value
  is nil, not int64` when the dataset contains a table with pseudo-columns,
  such as the `_PARTITIONTIME` column of an ingestion-time partitioned table.
  Pseudo-columns are left out of discovered schemas, since `SELECT *` never
  returns them.

## v1, 2023-11-13
- Beginning of changelog.
