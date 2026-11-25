# Changelog

## 2026-11-25

### Changed
- Starting a task no longer looks up every bound table.

## 2026-11-23

### Added
- Rows published before a backfill began are now deleted from the table once
  the backfill completes.

## 2026-11-03

### Changed
- Tasks split into multiple shards now fail to start with an error.
