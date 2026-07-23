# materialize-redshift

## 2026-07-23

### Changed
- When the `Exclude Flow Document` option is enabled for standard updates, the `_meta/uuid` field is now required to be materialized. This adds a `_meta/uuid` column to affected tables and includes the document UUID in materialized documents.

## v1, 2023-03-10
- Beginning of changelog.
