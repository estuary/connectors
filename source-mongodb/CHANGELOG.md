# Changelog

## 2026-08-05

### Fixed
- Captures now report an `unsupported collection type` error at startup when a
  captured database contains a collection of a type the connector does not
  recognize, instead of silently treating it as a standard collection.
