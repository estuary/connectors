# Changelog

## 2026-08-05

### Fixed
- `ticket_metric_events`, `ticket_skips`, and `ticket_activities` no longer drop most documents during incremental replication. Previously, only events that overflowed past the first page of each polling cycle were captured (~5% of the stream on a busy account). Backfills were not affected. Existing captures should re-backfill these bindings to recover the missing history.
