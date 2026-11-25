# Changelog

## 2026-11-25

### Fixed

- `Events` and `Persons` no longer skip rows when a page of results ends among rows that share one timestamp.
- `Persons` now captures persons added or seen after the first poll of each hour. PostHog rounds `last_seen_at` down to the hour, and the connector skipped persons that had the same value as its cursor. Re-backfill `Persons` to get the persons that were missed.
- `Events` no longer keeps every captured event in memory, which grew without limit and could run out of memory during a large backfill.
- The `Events` backfill now reads one day per query. A query over the whole range from the start date took longer than PostHog's 10-second query limit, so the backfill never finished.

## 2026-10-02

### Fixed

- `Events` no longer runs a second, redundant backfill per project, which emitted duplicate documents.

## 2026-09-29

### Added

- Added `Sessions` binding to capture PostHog sessions via HogQL.
