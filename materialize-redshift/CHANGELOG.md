# materialize-redshift

## 2026-07-23

### Changed
- When the `Exclude Flow Document` option is enabled for standard updates, materialized documents now include the document UUID at `_meta/uuid` — materialized directly when the `_meta/uuid` field is present (now required), or synthesized from `flow_published_at` otherwise (using a `1970-01-01` sentinel when that value is absent).

## v1, 2023-03-10
- Beginning of changelog.
