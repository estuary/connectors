# materialize-snowflake

## 2026-07-29

### Added
- New `snowpipe_streaming_v2` feature flag (off by default), which materializes
  delta-updates bindings through Snowflake's high-performance Snowpipe Streaming
  architecture. Requires key-pair (JWT) authentication.
- Rows on this path are streamed to Snowflake as they are materialized, so each
  row is transmitted once and no data is staged.
- The flag requires the task to run on the V2 materialization runtime, via the
  `enable-runtime-v2` shard flag. A task which sets the feature flag without the
  runtime flag is rejected when published, and refuses to start.
- The `snowpipe_streaming` and `snowpipe_streaming_v2` flags select different
  write paths and cannot both be enabled.
- Rows on this path become visible in the destination table slightly before the
  Flow transaction which produced them commits, and rows of a transaction which
  is interrupted before it commits remain in the table. Every transaction which
  does commit is delivered exactly once, including across restarts.
- A field which is absent or JSON `null` is stored as SQL NULL on this path,
  matching what the staged write paths produce for the same document — including
  for a VARIANT column, where SQL NULL and a VARIANT holding `null` answer
  `IS NULL` and `TYPEOF` differently.
- A row Snowflake's ingestion rejects on this path now fails the transaction,
  reporting the destination and Snowflake's own description of what it rejected,
  rather than acknowledging rows Snowflake did not store. Because rejected rows
  cannot be identified and re-sent, the failure holds until the binding is
  backfilled. Tables mark a column NOT NULL only for a field the collection
  schema requires, which the runtime therefore always supplies.
- If the connector cannot establish which rows Snowflake already holds on this
  path — because the destination lost committed data, or because the task's
  shards were split while an interrupted transaction's rows were outstanding —
  it fails rather than risk duplicating or dropping rows. Backfilling the
  affected binding recovers from either case.
- Splitting a task's shards needs no backfill. Reducing their number does:
  a shard which takes over the key range of another cannot establish which of
  that shard's rows Snowflake already holds, so bindings on this path fail until
  they are backfilled.

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
