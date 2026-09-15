# Changelog

## 2026-09-02

### Changed
- The connector checkpoint now groups each table's pending work under its
  binding first and the staging shard's key range second, so a task can find
  everything staged for a table across all of its shards in one place.
  Checkpoints written by earlier versions are still recovered and cleared.
  Note for downgrades: versions predating this change refuse to parse the new
  layout — drain the task (let it idle at an acknowledged commit) before
  pinning an older image.

## 2026-08-26

### Fixed
- A commit query failing with `COPY_INTO_DUPLICATED_FILES_COPY_NOT_ALLOWED`
  is now retried with backoff instead of restarting the connector. The
  conflicting query is one of a previous connector process that was killed
  while it was still running, and the retry succeeds once it has finished.

## 2026-08-25

### Added
- `1m`, `2m30s`, and `20m` are now valid `Sync Frequency` values, filling the
  gaps between `30s`-`5m` and `15m`-`30m`.

## 2026-08-20

### Changed
- The `scale_out` feature flag is removed: multi-shard operation is always
  enabled, since scaled-out tasks only ever run under the v2 runtime. A
  `scale_out` flag still present in a task's configuration is ignored.
- A task scaled out to multiple shards now commits each table with a single
  `MERGE` (or `COPY INTO`) query covering every shard's staged files, instead
  of running one query per shard back to back. Single-shard tasks are
  unaffected. The connector checkpoint now uses the range-scoped format for
  all tasks — a single-shard task's entries nest under its full key range —
  and records the staged files and key bounds instead of pre-rendered
  queries. Note for downgrades: versions predating this change refuse to
  parse the new format — drain the task (let it idle at an acknowledged
  commit) before pinning an older image.

## 2026-08-18

### Changed
- Files staged to the Unity Catalog volume are now gzip-compressed, which
  reduces the amount of data uploaded to Databricks at the cost of additional
  CPU usage.


## 2026-08-04

### Changed
- A task scaled out to multiple shards now fails to start unless the
  `scale_out` feature flag is enabled, instead of running in a mode that
  could crash-loop on concurrent `COPY INTO` operations or silently lose
  documents when shards are scaled back down. Tasks running a single shard
  are unaffected. If you see this error, enable the `scale_out` feature
  flag (requires runtime v2) or scale the task back to a single shard.
