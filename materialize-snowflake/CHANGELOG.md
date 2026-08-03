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
  runtime flag refuses to start, and fails before anything in Snowflake is
  created or altered for it. Publishing such a task is not itself rejected,
  because the specification a publication asks the connector to validate does not
  say which runtime the task will run; where the task has been running without
  the runtime flag, the publication's logs carry a warning naming the flag to
  add.
- The `snowpipe_streaming` and `snowpipe_streaming_v2` flags select different
  write paths and cannot both be enabled.
- Rows on this path become visible in the destination table slightly before the
  Flow transaction which produced them commits. What an interruption leaves
  behind turns on whether the channel those rows were streamed to is opened
  again. An interruption the task resumes from — a restart, a bounce, a topology
  change the connector accepts — reopens it, which discards the appends
  Snowflake had not yet committed while the replay accounts for the ones it had,
  so the interrupted transaction leaves no rows of its own behind. A channel
  which is instead abandoned keeps everything it had committed: that is what
  backfilling a binding leaves, since the backfill rotates the channel, and what
  deleting a task leaves. Every transaction which does commit is delivered
  exactly once, including across restarts.
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
  path — because the destination lost committed data, because rows outstanding
  beyond the last committed transaction were streamed for a key range other than
  the one the shard now covers, or because rows are already streamed for a shard's
  key range which the task's own record of what it has materialized cannot account
  for — it fails rather than risk duplicating or dropping rows. Backfilling the
  affected binding recovers from any of these.
- Changing how a task's shards are split needs no backfill — splitting them,
  reducing their number, or reducing and later increasing it again: a shard which
  takes over the key range of another settles that shard's rows before appending
  any of its own, and rows a shard streamed for its own key range are skipped by
  its replay however the checkpoint's record of that range has since changed.
  What is refused is a change made while rows another key range streamed are still
  outstanding, which is a transaction interrupted with rows in flight — near
  continuous while a binding is backfilling, since Snowflake commits batches of a
  transaction as it ingests them. A replay under the new ranges carries a
  different set of documents, so those rows would be materialized twice or
  dropped. The failure names the key range which streamed them, and restoring the
  task's shards to it lets that transaction commit, after which the change is
  clean; backfilling the binding also clears it.
- A shard arriving with a scale-up which finds rows already streamed for the key
  range it is given is refused too, since those rows belong to a shard which no
  longer exists.
- Backfilling a binding on this path while the task is running no longer
  duplicates rows in the destination.
- Backfilling a binding on this path drops and re-creates its table, rather than
  deleting the table's rows and keeping the table, so grants on that table do not
  survive the backfill.
- The `retain_existing_data_on_backfill` feature flag cannot be used with this
  path: backfilling a binding fails while both are enabled.
- A table on this path carries an extra line in its comment, recording which
  backfill generation of the binding materializes into it. A task which is still
  running a specification that a backfill has replaced fails, rather than adding
  rows to the table the backfill has re-created.

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
