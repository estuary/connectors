# materialize-redshift

## 2026-08-29

### Changed
- The destination commit now runs after the runtime commits its recovery log,
  in `Acknowledge`, rather than on the critical path of every transaction. The
  checkpoints table stops holding a fence and runtime checkpoint and instead
  records the token of each applied transaction, so a recovered transaction is
  applied exactly once, delta-updates bindings included. Existing tasks cross
  over automatically. Downgrading a task to an earlier image requires draining
  it first and would need a backfill, since the checkpoints row no longer
  advances the runtime checkpoint.

### Added
- Scale-out on the v2 runtime: every shard stages its own files to S3, and the
  shard whose key range begins at zero commits all shards' staged files for a
  table with a single COPY and MERGE per transaction.

## 2026-08-25

### Added
- `1m`, `2m30s`, and `20m` are now valid `Sync Frequency` values, filling the
  gaps between `30s`-`5m` and `15m`-`30m`.

## v1, 2023-03-10
- Beginning of changelog.
