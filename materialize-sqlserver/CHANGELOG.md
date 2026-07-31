# materialize-sqlserver

## 2026-07-23

### Changed
- When the `Exclude Flow Document` option is enabled, applying a source collection's backfill to a binding now requires that binding to materialize the `flow_published_at` field, which is what identifies the version of each existing row. It is selected by default and no columns are added to existing tables. Bindings without it continue to materialize as before, but fail with a clear error if a backfill is applied to them.

## v1, 2023-09-01
- Beginning of changelog.
