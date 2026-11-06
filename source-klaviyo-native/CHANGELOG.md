# Changelog

## 2026-08-31

### Fixed
- The events backfill's worker pool could declare a window complete while chunks were still in flight between queues, silently skipping unfetched date ranges. Completion is now tracked with exact outstanding-chunk accounting, and a residual accounting bug fails the window loudly instead of checkpointing past a hole.

### Changed
- Raised the events lookback horizon from 4 days to 7 days. On upgrade, existing tasks' lookback subtask idles for ~3 days while `now - horizon` catches back up to its cursor; this is expected and no data is skipped.

## 2026-07-30

### Fixed
- Datetime values Klaviyo returns space-separated (consent timestamps, the events datetime cursor, and custom properties) are now normalized to RFC3339, so materializations of fields inferred as `format: date-time` no longer fail with `cannot parse ... as "T"`.
