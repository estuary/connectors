# Changelog

## 2026-10-30

### Fixed
- The `internal_contacts` binding now requests every page of the company directory instead of stopping after the first 100 entries.
- The `user_call_log` and `messages` bindings now skip a re-fetched record only when that same record was already captured unchanged.
- The daily `user_call_log` lookback now captures updates to the call log records it re-fetches.
