# Changelog

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
