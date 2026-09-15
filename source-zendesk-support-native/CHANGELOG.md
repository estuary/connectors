# Changelog

## 2026-09-10

### Changed
- Incremental streams now trail roughly 5 minutes behind the present to combat the Zendesk Support API's eventually consistent behavior. Records still arrive on each binding's normal polling interval, with up to 5 minutes of additional delay.

## 2026-08-05

### Fixed
- `ticket_metric_events`, `ticket_skips`, and `ticket_activities` no longer drop the majority of records during incremental replication. Each poll discarded an entire page of results and often failed to advance its cursor, so these streams captured only a small fraction of new records once the initial backfill completed. Backfill was unaffected. Existing captures should backfill these bindings to recover the records that were missed.
