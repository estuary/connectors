# Changelog

## 2026-07-15

### Fixed
- Tables affected by a manifest-list encoding defect in a previous connector
  release are now repaired automatically when the materialization starts up.
  The defect could cause external query engines to reject a table's current
  snapshot (Redshift Spectrum: `Wrong type in Avro file. Field: partitions`)
  or to silently return NULL values for affected rows (Athena). No action is
  needed; the connector logs a warning for each table it repairs.
