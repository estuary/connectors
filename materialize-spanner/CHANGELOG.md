# materialize-spanner

## 2026-10-29

### Fixed
- Rows are no longer left with outdated values when a replaced instance of the
  materialization keeps running for a while.
- Materializations that share a database no longer fail or merge rows
  incorrectly because of one another.
