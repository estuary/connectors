# source-postgres-batch

## 2026-10-13

### Fixed
- `infinity` and `-infinity` values in date and timestamp columns are now captured as `9999-12-31T23:59:59Z` and `0000-01-01T00:00:00Z` instead of failing the capture.

## v1, 2023-07-31
- Beginning of changelog.
