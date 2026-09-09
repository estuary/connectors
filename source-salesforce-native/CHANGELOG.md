# Changelog

## 2026-09-09

### Fixed
- Setting `advanced.window_size` when first configuring a capture no longer fails with an
  `Unable to extract tag using discriminator 'window_type'` error. A window size saved without
  the window type selector is now resolved from the value that was provided.

### Changed
- The `advanced.window_size` setting is now labeled `Incremental Query Window Size`, and its
  duration option is labeled `Duration`, to distinguish it from the per-binding sync schedule.

## 2026-09-08

### Changed
- The date window strategy that drove backfills started before 2026-07-22 to completion has
  been removed, and the `advanced.window_size` setting now only applies to incremental
  catch-up sweeps.

## 2026-08-04

### Fixed
- Sourced schemas now mark every field they describe as required in addition to the `_meta` field.
- Sourced schemas now declare length bounds on string fields and range bounds on numeric fields.

## 2026-07-22

### Changed
- Backfills of incremental streams now paginate by the object's `Id` field instead of
  date windows over the cursor field. Backfill performance no longer depends on how
  records are distributed over time, and the `advanced.window_size` setting no longer
  needs tuning for large objects (it still applies to backfills already in progress
  and to incremental catch-up sweeps).
- Backfills that were already in progress before this change continue from their
  saved position using the previous date window strategy; no re-backfill is needed.
