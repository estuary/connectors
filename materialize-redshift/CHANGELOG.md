# materialize-redshift

## 2026-09-11

### Fixed
- Materializations sharing a metadata schema on a database using
  `SERIALIZABLE` isolation failed with `ERROR: 1023` (serializable isolation
  violation) when they committed concurrently, because the checkpoints table
  lock was taken after the transaction's snapshot. The lock is now the commit
  transaction's first statement, as it was before the post-commit apply
  change, and a serialization failure or deadlock during the commit is
  retried instead of failing the task.

## 2026-08-29

### Changed
- The destination commit now runs after the runtime commits its recovery log,
  in `Acknowledge`, rather than on the critical path of every transaction. The
  checkpoints table stops holding a fence and runtime checkpoint and instead
  records the token of each applied transaction, so a recovered transaction is
  applied exactly once, delta-updates bindings included. On the v2 runtime the
  task scales out: every shard stages its own files, and the shard whose key
  range begins at zero commits all shards' files for a table with a single COPY
  and MERGE per transaction. Existing tasks cross over automatically.
  Downgrading a task to an earlier image requires draining it first and would
  need a backfill, since the checkpoints row no longer advances the runtime
  checkpoint.

## 2026-08-25

### Added
- `1m`, `2m30s`, and `20m` are now valid `Sync Frequency` values, filling the
  gaps between `30s`-`5m` and `15m`-`30m`.

## v1, 2023-03-10
- Beginning of changelog.
