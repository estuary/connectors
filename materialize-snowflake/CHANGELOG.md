# materialize-snowflake

## 2026-09-04

### Added
- New `snowpipe_streaming_v2` feature flag (off by default). Delta-updates
  bindings stream their rows to Snowflake through the high-performance Snowpipe
  Streaming architecture as they are materialized, so nothing is staged and each
  row is transmitted once. Requires key-pair (JWT) authentication and the
  `enable-runtime-v2` shard flag. A task missing either, or which also sets
  `snowpipe_streaming`, is rejected before anything in Snowflake is created or
  altered. The runtime flag can only be checked once the task runs, so a
  publication of a task without it logs a warning naming the flag.

### Changed
With `snowpipe_streaming_v2` set, delta-updates bindings behave as follows.
- Each binding writes through four channels per shard, one per equal subrange
  of the shard's key-hash range, for a ceiling of 80 MB/s per shard. A shard
  split or join hands each surviving shard whole channels, which resume from
  their committed offset tokens, so a topology change loses nothing.
- Rows become visible in the destination slightly before the Flow transaction
  which produced them commits. Every committed transaction is still
  materialized exactly once, including across restarts.
- Backfilling a binding drops and re-creates its table rather than deleting its
  rows, so grants on that table do not survive the backfill, and the
  `retain_existing_data_on_backfill` feature flag does not apply to it. Other
  bindings of the task are unaffected.
- A binding moving onto this path first finishes the work its previous write
  path staged. Where that is impossible, the binding is refused, naming the
  table and the count outstanding.
- Two tasks may not stream into one table; the second is refused, naming the
  first. A renamed task is refused the same way until the binding is
  backfilled.
- The one way off this path without a backfill is setting `snowpipe_streaming`
  while removing `snowpipe_streaming_v2`, on a task that keeps the V2 runtime.
  Documents Snowflake had committed beyond the checkpoint are then materialized
  again. Any other departure is rejected, naming the binding.
- A row Snowflake's ingestion rejects fails the transaction, reporting
  Snowflake's own description of what it rejected. Rejected rows cannot be
  re-sent, so the failure holds until the binding is backfilled.
- If the connector cannot establish which rows Snowflake already holds, such as
  after the same shard is split twice before its channels converged, it fails
  rather than duplicate or drop rows. Backfilling the binding recovers.

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
