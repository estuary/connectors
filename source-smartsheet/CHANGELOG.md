# Changelog

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
