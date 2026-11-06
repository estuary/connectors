# Changelog

## 2026-11-04

### Added
- Backfills of the `issues` binding and its child bindings can be set to delete the destination rows they don't capture again.
- FEATURE FLAG SWITCH: Starting 2026-11-16 the feature flag emit_backfill_signals will be on by default for new tasks, while existing tasks created at or before 2026-11-15 will keep their existing behaviour. Backfills of the `issues` binding and its child bindings delete the destination rows they don't capture again when `start_date` is `1970-01-01T00:00:00Z` and `projects` is empty.
