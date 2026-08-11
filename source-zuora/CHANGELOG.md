# Changelog

## 2026-08-13

### Added
- `AchNocEventLog` is now captured incrementally. It is the only object whose change
  timestamp is `UpdatedOn` rather than `UpdatedDate`, so it previously matched no cursor
  and was re-exported in full on every poll.

  The binding's key changes from `/_meta/row_id` to `/Id`, and a data flow reset for the binding is required as a result of the key change.

## 2026-08-04

### Added
- Exports now select each object's related-object ids, so documents carry the
  foreign keys that let them be joined to the objects they belong to
  (`InvoiceItem.InvoiceId`, `RatePlan.SubscriptionId`, `Invoice.AccountId`, ...).
  Each joinable related object contributes one `<Name>.Id` to the query, landing in documents as `<Name>Id`.

## 2026-07-22

### Changed
- Backfills now paginate by `Id` (`WHERE Id > :last ORDER BY Id LIMIT 250000`) instead of walking 30-day cursor-field windows. An oversized page halves its `LIMIT` and retries.

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
