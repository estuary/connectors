# Changelog

## 2026-07-22

### Fixed
- Captures no longer fail on populated `CURRENCY` and `PERCENT` fields. These
  values are now emitted as numeric strings (like `DECIMAL`) instead of raising
  `Unhandled datatype ... for ...`, and their fields are included in the
  captured document schema.
