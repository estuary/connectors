# Changelog

## 2026-07-17

### Fixed
- Captures no longer crash when a timestamp folder's `model.json` is present but does not list an entity for a given binding. The connector now falls back to the per-table `model.json` that Synapse Link writes under `Microsoft.Athena.TrickleFeedService/`.
- CSV rows whose column count doesn't match their table's schema now fail with a clear error instead of being silently padded or truncated, guarding against misaligned reads from a fallback per-table `model.json`.

## 2026-07-16

### Fixed
- Captures no longer crash when an older timestamp folder is missing its `model.json`. Such folders come from Synapse Link exports that never finalized; they are now skipped once enough time has passed for finalization, and their data is picked up from the later folder Synapse Link re-commits it to.
