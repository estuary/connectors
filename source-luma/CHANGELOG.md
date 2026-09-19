# Changelog

## 2026-09-17

### Added
- Initial release of the Luma capture connector. Captures `calendars`, `events`,
  `guests`, and `membership_tiers` from a Luma calendar API key. Every stream is a
  snapshot: Luma exposes no modification timestamps or change filters, so each
  binding re-lists its resource every interval and deletions are inferred.
