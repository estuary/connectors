# Changelog

## 2026-07-22

### Fixed
- Populated `CURRENCY` and `PERCENT` fields no longer fail the capture with an
  "Unhandled datatype" error. Their values are now captured as number-formatted
  strings, the same as `DECIMAL` fields.
