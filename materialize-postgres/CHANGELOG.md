# materialize-postgres

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
