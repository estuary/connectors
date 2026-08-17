# Changelog

## 2026-08-17

### Changed
- A task scaled out to multiple shards now commits each table with a single
  `MERGE` (or `COPY INTO`) query covering every shard's staged files, instead
  of running one query per shard back to back. Single-shard tasks are
  unaffected. Note for downgrades: the connector checkpoint now records the
  staged files alongside the pre-rendered queries, and versions predating this
  change refuse to parse the new fields — drain the task (let it idle at an
  acknowledged commit) before pinning an older image.

## 2026-08-04

### Changed
- A task scaled out to multiple shards now fails to start unless the
  `scale_out` feature flag is enabled, instead of running in a mode that
  could crash-loop on concurrent `COPY INTO` operations or silently lose
  documents when shards are scaled back down. Tasks running a single shard
  are unaffected. If you see this error, enable the `scale_out` feature
  flag (requires runtime v2) or scale the task back to a single shard.
