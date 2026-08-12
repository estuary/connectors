# Changelog

## 2026-08-12

### Added

- New `skipBackfills` advanced option: a comma-separated list of collections,
  whose backfill is skipped so that only their change events are captured.

## 2026-08-05

### Fixed

- Captures now report an `unsupported collection type` error at startup when a
  captured database contains a collection of a type the connector does not
  recognize, instead of silently treating it as a standard collection.
