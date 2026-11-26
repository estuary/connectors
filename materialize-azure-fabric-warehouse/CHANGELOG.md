# Changelog

## 2026-11-24
### Added
- Load, commit and truncation queries that run for at least five seconds are
  logged with the metrics Fabric records of them in query insights: time
  spent waiting to start and running, CPU time, rows, the data they scanned
  from remote storage, memory and disk, and whether their SQL pool was under
  pressure. A load also logs the keys it looked up in each table and the key
  bounds it was restricted to, and a merge its key bounds. The first slow
  query of each table in a run also logs how many statistics the table has
  and when they were last updated, and the warehouse's version, capacity and
  settings. Query insights records a query a few minutes after it finishes,
  so the metrics are read in the background and the query is logged then.
  To find a query in query insights, the connector now labels each of these
  queries with `OPTION (LABEL = '...')`.

## 2026-11-11
### Fixed
- Retry installing the fence and committing a transaction when Fabric
  Warehouse rolls it back on a deadlock or snapshot-isolation conflict,
  which can happen when multiple tasks write to the same schema.

## 2026-11-06
### Fixed
- Only populate the info schema from the tables that are referenced by this
  materialization's bindings.  This can crashes if there are multiple tasks
  materializing to the same schema.

## 2026-11-05
### Added
- A completed backfill now deletes a table's rows published before the
  backfill began, instead of leaving them in place until the next store
  overwrites or removes them. Controlled by the `truncate_after_backfill`
  feature flag, enabled by default; a binding without a `flow_published_at`
  column, or in delta-updates mode, is unaffected.

## 2026-11-03

### Changed
- Tasks split into multiple shards now fail to start with an error.
