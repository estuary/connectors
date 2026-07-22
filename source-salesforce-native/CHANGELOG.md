# Changelog

## 2026-07-22

### Changed
- Backfills of incremental streams now paginate by the object's `Id` field instead of
  date windows over the cursor field. Backfill performance no longer depends on how
  records are distributed over time, and the `advanced.window_size` setting no longer
  needs tuning for large objects (it still applies to backfills already in progress
  and to incremental catch-up sweeps).
- Backfills that were already in progress before this change continue from their
  saved position using the previous date window strategy; no re-backfill is needed.
