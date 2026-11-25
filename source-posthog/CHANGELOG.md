# Changelog

## 2026-11-25

### Fixed

- `Events` and `Persons` no longer skip rows that share a timestamp with the end of a page of results.
- `Persons` now captures persons added later in the same hour as the previous poll.
- `Persons` now captures changes made to a person later in the same hour.
- `Events` and `Persons` no longer delay or skip data after PostHog receives an event dated in the future.
- `Events` and `Sessions` backfills no longer lose a document when they pause to checkpoint.
- `Events` keeps only recent events in memory instead of every event it has captured. Memory use grew without limit during a large backfill.

### Changed

- The `Events` backfill reads one day of events per query instead of the whole range since the start date. This may let the backfill finish on projects where a query over the whole range took longer than PostHog's 10-second limit.
- `Events` and `Sessions` wait a full interval between polls instead of polling back to back on busy projects.
- `Persons` now emits every person seen in the last one to two hours on each poll.

## 2026-10-02

### Fixed

- `Events` no longer runs a second, redundant backfill per project, which emitted duplicate documents.

## 2026-09-29

### Added

- Added `Sessions` binding to capture PostHog sessions via HogQL.
