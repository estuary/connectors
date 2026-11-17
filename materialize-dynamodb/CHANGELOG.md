# materialize-dynamodb

## 2026-11-16

### Added
- A completed backfill now deletes a table's rows published before the
  backfill began, instead of leaving them in place until the next store
  overwrites or removes them.

## 2026-11-03

### Changed
- Tasks split into multiple shards now fail to start with an error.

## v1, 2023-08-25

- Beginning of changelog.
