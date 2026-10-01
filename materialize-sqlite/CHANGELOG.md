# materialize-sqlite

## 2026-10-01

### Fixed
- Updating an existing row no longer fails with a syntax error when the table has no columns besides its keys and `flow_document`.
- Restarting the connector against an existing database file no longer fails with `table flow_temp_table_<n> already exists`.

## v1, 2023-03-10

- Beginning of changelog.
