# Changelog

## 2026-08-17

### Fixed

- Servers that report a recent version but reject the
  `$changeStreamSplitLargeEvent` pipeline stage, such as Amazon DocumentDB 8, no
  longer fail the capture at startup. They are now captured without pre-images,
  matching the existing behavior for MongoDB Atlas deployments that reject the
  stage.

## 2026-08-12

### Added

- New `skipBackfills` advanced option: a comma-separated list of collections,
  whose backfill is skipped so that only their change events are captured.

## 2026-08-05

### Fixed

- Captures now report an `unsupported collection type` error at startup when a
  captured database contains a collection of a type the connector does not
  recognize, instead of silently treating it as a standard collection.
