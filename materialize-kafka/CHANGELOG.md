# materialize-kafka

## 2026-09-22

### Changed
- Avro schemas registered by newly created materializations now carry their
  Avro logical types. `flow_published_at`, and any other selected field with
  `format: date-time`, is registered as a `long` with `logicalType:
  timestamp-micros` instead of a plain `string`, so consumers can read it as a
  timestamp. Materializations that existed before this change keep their
  current schemas, so consumers reading those topics are unaffected. To adopt
  the new schemas, create a new materialization.

### Added
- New `advanced.feature_flags` endpoint configuration field. Setting it to
  `no_avro_logical_types` when creating a materialization keeps date-time
  fields as plain `string` values in its Avro schemas, for example to match
  topics written by an older materialization. The flag is read only when the
  materialization is first created.
