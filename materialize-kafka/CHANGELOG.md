# materialize-kafka

## 2026-09-22

### Added
- New `avro_logical_types` endpoint configuration field, enabled by default
  for new materializations. When enabled, registered Avro schemas carry their
  Avro logical types: `flow_published_at`, and any other selected field with
  `format: date-time`, is registered as a `long` with `logicalType:
  timestamp-micros` instead of a plain `string`, so consumers can read it as a
  timestamp. Materializations created before this field existed do not have
  it set and keep their current schemas, so consumers reading those topics are
  unaffected. Enabling it on an existing materialization changes the type of
  those fields in its topics.
