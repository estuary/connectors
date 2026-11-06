# Changelog

## 2026-08-03

### Changed

- **Breaking:** rewrote the connector natively on `estuary-cdk`, replacing the
  imported Airbyte declarative source. Existing captures must be reconfigured:

  - The API key moved from the top-level `api-key` property into a
    `credentials` object (`credentials_title: "API Key"`, `access_token`).
  - The `start_date` property was removed.
  - Stream names are now lower-snake-case (`Contacts` → `contacts`,
    `ContactsAttributes` → `contacts_attributes`, `ContactsLists` →
    `contacts_lists`), so bindings must be retargeted.

### Added

- `contacts_folders`, `contacts_segments`, `senders`, and `webhooks` streams.
- Incremental replication for `contacts`, filtered on `modifiedAt`, with a separate historical backfill.
