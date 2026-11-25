# Changelog

## 2026-11-25

### Fixed

- `Events` and `Persons` no longer skip rows when a page of results ends among rows that share one timestamp.

## 2026-10-02

### Fixed

- `Events` no longer runs a second, redundant backfill per project, which emitted duplicate documents.

## 2026-09-29

### Added

- Added `Sessions` binding to capture PostHog sessions via HogQL.
