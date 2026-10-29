# Changelog

## 2026-10-29

### Changed
- Analytics bindings read daily data from all of an app's active report requests instead of only the first one Apple lists.
- Analytics backfills only use a snapshot Apple made on or after the day the backfill started. If none exists, the backfill requests one and waits for it. Apple allows one new snapshot per app per month, so this wait can last up to a month.
