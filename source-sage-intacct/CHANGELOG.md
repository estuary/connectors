# Changelog

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
