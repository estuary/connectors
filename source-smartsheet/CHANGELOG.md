# Changelog

## 2026-08-24

### Fixed

- Responses are now requested with `Accept-Encoding: identity` instead of gzip.
  Smartsheet can gzip a response twice while a single layer of encoding is
  expected, so the body left after the client's one decompression pass is still
  compressed and fails to parse as JSON.

## 2026-08-13

### Fixed

- The `reports` and `report_rows` captures no longer fail when the number of reports
  or report rows decreases. Previously the deletion record written for a removed row
  was rejected, failing the entire capture pass and discarding that pass's valid
  documents along with it.

### Changed

- The `reports` and `report_rows` collections are now keyed on `/_meta/row_id`, and are
  discovered with a minimal write schema instead of declaring a required `id`.

## 2026-07-22

### Added

- Initial release of the Smartsheet capture connector.
