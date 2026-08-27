# Changelog

## 2026-08-26

### Fixed

- Captures of databases with pre-images enabled no longer fail permanently with
  `received fragment N without first fragment`. MongoDB delivers change events
  larger than 16MB as a series of fragments, and the connector could checkpoint
  a resume token pointing partway through one of them. Restarting from that
  position delivers only the remaining fragments, which cannot be reassembled
  into the original event, so every subsequent restart failed the same way.
  Resume tokens are now only checkpointed at a position between whole events.

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
