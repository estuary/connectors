# materialize-snowflake

## 2026-08-28

### Added
- New `snowpipe_streaming_v2` feature flag (off by default), which materializes
  delta-updates bindings through Snowflake's high-performance Snowpipe Streaming
  architecture. Rows are streamed as they are materialized, so each row is
  transmitted once and nothing is staged. Requires key-pair (JWT)
  authentication, and requires the task to run on the V2 materialization runtime
  via the `enable-runtime-v2` shard flag.
- Each binding on this path writes through four channels per shard, one per
  equal subrange of the shard's key-hash range, with each document routed by
  the same key hash the runtime routes documents to shards by. Snowflake meters
  throughput per channel, so a shard's ceiling is 80 MB/s. Because a channel's
  contents depend only on the data, a shard split or join hands each surviving
  shard whole channels, and the shard continues them where their committed
  offset tokens left off — including the rows of a transaction the topology
  change interrupted — before converging back to four channels of its own
  range at the next settled transaction boundary.
- Rows on this path become visible in the destination table slightly before the
  Flow transaction which produced them commits. Every transaction which does
  commit is still materialized exactly once, including across restarts.
- A binding moving onto this path finishes the work its previous write path staged
  and did not finish, rather than refusing until an operator drains it. The binding
  registers with the previous path's manager for that one drain, while its rows go
  to this path. Where that registration is impossible — a table the previous path
  cannot stream into — the binding is still refused, naming the table and the count
  outstanding.
- Two tasks may not stream into one table on this path. A task whose table records
  another materialization fails rather than append to it, because each task's
  channels account only for the documents it appended itself, and a backfill of
  either task drops the table and every channel appending to it. A task that was
  renamed reaches the same refusal, and a backfill of the binding clears it by
  re-creating the table under the new name.

### Changed
- A task which enables `snowpipe_streaming_v2` without the `enable-runtime-v2`
  shard flag, without key-pair authentication, or alongside `snowpipe_streaming`,
  is rejected before anything in Snowflake is created or altered for it. The
  runtime requirement cannot be checked when the task is published, because a
  publication does not say which runtime the task will run under; where the task
  has been running without the runtime flag, the publication's logs carry a
  warning naming the flag to add.
- Backfilling a delta-updates binding on this path drops and re-creates its
  table rather than deleting the table's rows, so grants on that table do not
  survive the backfill, and the `retain_existing_data_on_backfill` feature flag
  cannot be used for those bindings. A task's other bindings are unaffected.
- A binding which has materialized on this path may not be moved off it, whether
  by disabling the feature flag, by changing the binding away from delta updates,
  or by changing the task's authentication. The publication which would move it
  is rejected, naming the binding; a task which has already been moved fails when
  it next starts rather than silently dropping rows on a later return to this
  path. Backfilling the binding is the way off, and moving a binding onto this
  path is unaffected.
- If the connector cannot establish which rows Snowflake already holds on this
  path, it fails rather than risk duplicating or dropping rows. Changing how a
  task's shards are split does not fail the binding, even while rows are in
  flight; what fails is a topology under which rows cannot be attributed, such
  as splitting the same shard twice before its channels converged. Backfilling
  the affected binding recovers from any such failure.

### Fixed
- A row Snowflake's ingestion rejects on this path now fails the transaction,
  reporting the destination and Snowflake's own description of what it rejected,
  rather than acknowledging rows Snowflake did not store. Rejected rows cannot be
  identified and re-sent, so the failure holds until the binding is backfilled.
- A field which is absent or JSON `null` is stored as SQL NULL on this path,
  matching what the staged write paths produce for the same document.
- Backfilling a binding on this path while the task is running no longer
  duplicates rows in the destination, and a task still running a specification
  that a backfill has replaced now fails rather than adding rows to the
  re-created table.

## 2026-08-25

### Added
- `1m`, `2m30s`, and `20m` are now valid `Sync Frequency` values, filling the
  gaps between `30s`-`5m` and `15m`-`30m`.

## 2026-08-24

### Fixed
- A `number` column no longer fails the transaction when it receives a whole
  number larger than the `int64` maximum. These values arrive as `uint64` or
  `big.Int`, and the connector now converts them to a float.

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
