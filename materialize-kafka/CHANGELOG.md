# materialize-kafka

## 2026-09-21

### Changed
- Avro schemas registered by newly created materializations now carry their
  Avro logical types. `flow_published_at`, and any other selected field with
  `format: date-time`, is registered as a `long` with `logicalType:
  timestamp-micros` instead of a plain `string`, so consumers can read it as a
  timestamp. Materializations that existed before this change keep their
  current schemas, so consumers reading those topics are unaffected. To adopt
  the new schemas, create a new materialization.
