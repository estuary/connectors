# Changelog

## 2026-07-21

### Changed
- Exports now run through Zuora's AQuA API (`POST /v1/batch-query/`, stateless
  mode) instead of the legacy `/v1/object/export` API. Some tenants
  feature-gate fields (e.g. `Account.DefaultPaymentMethodId`) out of the legacy
  export engine even though the describe API marks them exportable, failing
  captures with "There is no field named X"; AQuA accepts those fields. Export
  ZOQL queries, cursor state, and bindings are unchanged. Datetime output is
  pinned to UTC via `dateTimeUtc`, and segmented result files (tenants with
  AQuA file segmentation enabled) are supported.

## 2026-07-17

### Added
- Initial release of the Zuora capture connector.
