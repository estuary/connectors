# materialize-bigquery

## 2026-07-23

### Changed
- When the `Exclude Flow Document` option is enabled for standard updates, materialized documents now include the document UUID at `_meta/uuid` — materialized directly when the `_meta/uuid` field is present (now required), or synthesized from `flow_published_at` otherwise (using a `1970-01-01` sentinel when that value is absent).

## 2026-07-18

### Fixed
- Fixed a permanent `403 Access Denied: ... does not have permission to access policy
  tag ... on column flow_temp_table_N.cN` error when materializing to tables that use
  BigQuery column-level security (policy tags). The connector's internal staging table
  no longer inherits policy tags from destination columns; BigQuery enforced access
  control on that staging table regardless of the service account's Fine-Grained
  Reader grants, so no permissioning change could resolve the error.

## v1, 2022-07-27
- Beginning of changelog.
