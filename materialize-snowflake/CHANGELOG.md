# materialize-snowflake

## 2026-07-27

### Added
- Support for Snowflake's high-performance Snowpipe Streaming SDK, enabled
  with the `snowpipe_streaming_v2` feature flag (off by default). The official
  Snowflake SDK runs in a supervised sidecar process alongside the connector
  and applies to delta-updates bindings using key-pair (JWT) authentication.

  This flag requires the task to run on the V2 materialization runtime, via the
  `enable-runtime-v2` shard flag. A task that sets the feature flag without the
  runtime flag is rejected when published and refuses to start. The
  `snowpipe_streaming` and `snowpipe_streaming_v2` flags select different write
  paths and cannot both be enabled.

  Rows are streamed to Snowflake as they are materialized, so each row is
  transmitted once and no data is staged. Two consequences are worth planning
  for: rows become visible in the destination table slightly before the Flow
  transaction which produced them commits, and rows of a transaction that is
  interrupted before it commits remain in the table. Every transaction which
  does commit is delivered exactly once, including across restarts.

  A field which is absent or JSON `null` is stored as SQL NULL, matching what
  the staged write paths produce for the same document — including for a
  VARIANT column, where SQL NULL and a VARIANT holding `null` answer `IS NULL`
  and `TYPEOF` differently.

  A row Snowflake's ingestion rejects — a null against a NOT NULL column, for
  instance — is discarded without the append or the commit failing, and the
  channel's committed position advances past it. The connector consults the count
  of rejected rows Snowflake reports, as each transaction commits and again
  whenever it resumes writing to a table, and fails rather than acknowledging
  rows Snowflake did not store — reporting the destination and Snowflake's own
  description of what it rejected. Because the discarded rows cannot be
  identified and re-sent, the failure holds until the binding is backfilled,
  instead of a retry quietly carrying on with the rows missing. The connector's
  tables mark a column NOT NULL only for a field the collection schema requires,
  which the runtime therefore always supplies.

  If the connector cannot establish which rows Snowflake already holds — because
  the destination lost committed data, or because the task was rescaled while an
  interrupted transaction's rows were outstanding — it fails rather than risk
  duplicating or dropping rows. Backfilling the affected binding recovers from
  either case.

## 2026-07-23

### Changed
- Schema changes now first commit any transaction that was staged but not yet
  fully applied to the tables they affect, instead of leaving it to be applied
  afterwards. This prevents failures where staged data built against the
  previous table schema could no longer be applied after a column was added,
  made nullable, or had its type migrated.

### Fixed
- With the `retain_existing_data_on_backfill` feature flag enabled, backfilling
  a binding no longer risks losing the rows of a transaction that was committed
  but not yet fully applied to the destination: the pending transaction is now
  applied before the backfill takes effect.

## v1, 2022-07-27
- Beginning of changelog.
