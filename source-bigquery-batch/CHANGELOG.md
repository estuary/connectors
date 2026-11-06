# source-bigquery-batch

## 2026-11-02

### Changed
- Transactions containing sourced schemas are checkpointed immediately instead of after the connector's first poll. This may make task publications more responsive in certain setups.

## 2026-09-22

### Fixed
- Datasets containing an ingestion-time partitioned table no longer fail with
  `panic: interface conversion: bigquery.Value is nil, not int64`. The panic came
  from the table's `_PARTITIONTIME` pseudo-column during column discovery.
  Pseudo-columns are not included in discovered schemas.
