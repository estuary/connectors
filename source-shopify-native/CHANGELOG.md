# Changelog

## 2026-09-24

### Changed
- Captures keep running when a Shopify store is frozen (HTTP 402), locked (423), or no
  longer exists (404). Such a store is skipped with a warning and resumes from its last
  persisted cursor once it is reachable again after a future connector restart.
- A binding for a stream that no configured store can serve now opens idle with a warning
  instead of failing validation and the capture.

## 2026-08-27

### Added
- `orders` line items now include `name`, the line item's title as it appears
  on the order (the product title, with the variant title appended when the
  variant has one). This identifies what sold on orders whose line items were
  entered without a `sku`.

## 2026-08-19

### Fixed
- `products`, `custom_collections`, and `smart_collections` no longer fail
  entirely for stores whose access token doesn't grant `read_publications`.
  The connector now omits publication data instead of failing the whole
  export when that scope is missing.

## 2026-08-18

### Added
- `order_returns` now captures each return's `returnShippingFees` (the shipping
  fee charged on the return) and each return line item's `fulfillmentLineItem`
  (tying it back to the order's original line item).

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
