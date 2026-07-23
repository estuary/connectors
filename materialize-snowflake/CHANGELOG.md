# materialize-snowflake

## 2026-07-17

### Added
- Support for Snowflake's high-performance Snowpipe Streaming SDK, enabled
  with the `snowpipe_streaming_v2` feature flag (off by default). The official
  Snowflake SDK runs in a supervised sidecar process alongside the connector
  and applies to delta-updates bindings using key-pair (JWT) authentication.

## v1, 2022-07-27
- Beginning of changelog.
