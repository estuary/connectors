# Changelog

## 2026-11-25

### Fixed

- `Events` and `Persons` no longer skip rows that share a timestamp with the end of a page of results.
- `Persons` now captures persons added later in the same hour as the previous poll.
- `Persons` now captures changes made to a person later in the same hour.
- `Events` and `Persons` no longer delay or skip data after PostHog receives an event dated in the future.
- `Events` and `Sessions` backfills no longer lose a document when they pause to checkpoint.
- `Events` no longer runs out of memory during a large backfill.
- The `Events` backfill now finishes on projects with a long history.

### Changed

- `Events`, `Sessions` and `Persons` send fewer queries to PostHog on busy projects.

## 2026-10-02

### Fixed

- `Events` no longer runs a second, redundant backfill per project, which emitted duplicate documents.

## 2026-09-29

### Added

- Added `Sessions` binding to capture PostHog sessions via HogQL.
