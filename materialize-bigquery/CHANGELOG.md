# materialize-bigquery

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
