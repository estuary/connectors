# Changelog

## 2026-07-16

### Fixed
- Captures no longer crash when an older timestamp folder is missing its `model.json`. Such folders come from Synapse Link exports that never finalized; they are now skipped once enough time has passed for finalization, and their data is picked up from the later folder Synapse Link re-commits it to.
