# Changelog

## 2026-08-04

### Fixed
- Sourced schemas now describe the `_meta` field.
- Sourced schemas now declare length bounds on string fields and range bounds on numeric fields.

## 2026-07-22

### Fixed
- Populated `CURRENCY` and `PERCENT` fields no longer fail the capture with an
  "Unhandled datatype" error. Their values are now captured as number-formatted
  strings, the same as `DECIMAL` fields.
