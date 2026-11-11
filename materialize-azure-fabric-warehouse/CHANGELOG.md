# Changelog

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
