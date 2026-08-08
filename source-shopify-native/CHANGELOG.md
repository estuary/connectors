# Changelog

## 2026-08-08

### Added
- New `markets` stream capturing each Shopify Market, with that market's
  market catalogs embedded as a `catalogs` list. Requires the `read_markets`
  access scope. The stream is skipped for stores that don't grant it.

### Changed
- `locations` and `location_metafields` now include deactivated locations.
  Existing captures pick up a deactivated location once its `updatedAt`
  advances. Locations deactivated before this release require a backfill of
  those bindings to appear.
