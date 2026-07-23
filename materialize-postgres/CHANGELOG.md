# materialize-postgres

## 2026-07-23

### Changed
- When the `Exclude Flow Document` option is enabled for standard updates, materialized documents now include the document UUID at `_meta/uuid` — materialized directly when the `_meta/uuid` field is present (now required), or synthesized from `flow_published_at` otherwise (using a `1970-01-01` sentinel when that value is absent).

## 2026-07-20

### Fixed
- Checkpoint fence updates are now issued as a parameterized `UPDATE` instead of a `DO` block with the checkpoint inlined as a literal. Because a `DO` block is a utility statement whose body cannot be normalized, every commit previously created a distinct `pg_stat_statements` entry, which could exhaust `pg_stat_statements.max` and cause `LWLock:pg_stat_statements` contention on the destination. The parameterized statement collapses into a single normalized entry.

## v4, 2022-11-30

This version includes breaking changes to materialized table columns. These will provide more
consistent column names and types, but tables created from previous versions of the connector may
not be compatible with this version:
- Properly quote projected field names so that column names match field names instead of being
  converted to lowercase.
- Convert formats of `date`, `duration`, `ipv4`, `ipv6`, `macaddr`, `macaddr8`, and `time` into
  their corresponding postgres types when creating columns and materializing values. Previously,
  only `date-time` was converted, and all others were materialized as strings.

## v3, 2022-10-13
- Switch to using TCP for communicating with Flow runtime and standalone network-tunnel

## v2, 2022-07-28
- Merged `host` and `port` configuration fields into `address`. https://github.com/estuary/connectors/pull/297

## v1, 2022-07-27
- Beginning of changelog.
