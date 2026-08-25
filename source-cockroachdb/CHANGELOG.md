# source-cockroachdb Changelog

## v1 - Initial Release

Real-time CDC capture from CockroachDB using native sinkless ("core") changefeeds streamed over
the SQL connection. Supports:

- Discovery of tables, columns, and (single or composite) primary keys, including tables keyed on
  the synthesized `rowid` and hash-sharded primary keys.
- Chunked initial backfill reading `AS OF SYSTEM TIME` the latest resolved timestamp, so
  concurrent changes always supersede backfilled row states.
- Streaming of inserts, updates, and deletes (including hard-delete reduction), routed by fully
  qualified table name.
- Resumption from a persisted HLC cursor with no re-snapshot.
- Validation-time rejection of tables the connector can't capture correctly (floating-point
  primary keys, `REGIONAL BY ROW`, multiple column families).
