# Changelog

## 2026-11-19

### Added

- New General Ledger bindings `GLBATCH` (journal entries) and `GLENTRY`
  (journal entry lines). They include the records made inside an entity of a
  multi-entity company.
- Backfills: each binding has a new optional `schedule` setting, to backfill
  it on a cron schedule. Backfills of the General Ledger bindings read all
  entries. They also remove the rows that no longer exist in Sage from the
  destination, for captures created on or after 2026-12-01.

### Fixed

- Bindings with more than 2,000 records modified in the same second now send
  a valid request for the records after the first 2,000. Its two filter
  conditions are combined with `<and>`, as Sage requires.

## 2026-09-10

### Fixed

- A deletion whose AUDITHISTORY `OBJECTKEY` is not a `RECORDNO` no longer
  crashes the capture. Such deletions are skipped with a debug log.

## 2026-08-21

## Fixed

- Bindings' default polling intervals increased from 5 minutes to 6 hours to reduce the frequency of API calls.
  This change only affects newly discovered bindings. Existing captures should update their resource configs
  if they want to poll for changes less frequently.

## 2026-08-04

### Fixed

- Sourced schemas now describe the `_meta` field.
- Sourced schemas now declare length bounds on string fields and range bounds on numeric fields.

## 2026-07-22

### Fixed

- Populated `CURRENCY` and `PERCENT` fields no longer fail the capture with an
  "Unhandled datatype" error. Their values are now captured as number-formatted
  strings, the same as `DECIMAL` fields.
