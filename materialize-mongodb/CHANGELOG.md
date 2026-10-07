# materialize-mongodb

## 2026-10-07

### Added
- When a collection is backfilled, documents the backfill did not re-send are
  deleted from standard-updates collections once it completes, so documents
  removed from the source no longer linger in the destination. Delta-updates
  collections keep every document. Each stored document now carries its
  publication time in a `_flow_published_at` field, and documents stored
  before this version have no such field, so they are kept.
  Disable it with the `no_truncate_after_backfill` feature flag.

## 2026-09-25

### Fixed

- Connection errors no longer include the password when one was entered as part
  of the address. The address is shown with the password masked, and address
  parsing errors no longer repeat the address.

## v1, 2023-03-01
- Beginning of changelog.
