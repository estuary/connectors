# Changelog

## 2026-11-10

### Fixed
- Bookings backfills set both ends of their date range instead of only the end. This may stop Navan from rejecting backfill requests.
- Incremental syncs read every page of changed bookings instead of only one page.

### Changed
- Bookings requests start at page 0 and set the page size with `size`, as Navan's API reference documents. This may stop syncs from skipping the first page of bookings.
- Bookings date filters are sent as epoch seconds, as Navan's API reference documents.
- Bookings backfills filter by creation time instead of update time. This may prevent skipping bookings that are updated during a backfill.
- Incremental syncs only read bookings modified before the current second. This may prevent skipping bookings modified in the same second as a sync.
