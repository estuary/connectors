# Changelog

## 2026-08-18

### Added
- New advanced option `compress_staged_files`. When enabled, files staged to
  the Unity Catalog volume are gzip-compressed, which reduces the amount of
  data uploaded at the cost of additional CPU usage. Disabled by default.

## 2026-08-04

### Changed
- A task scaled out to multiple shards now fails to start unless the
  `scale_out` feature flag is enabled, instead of running in a mode that
  could crash-loop on concurrent `COPY INTO` operations or silently lose
  documents when shards are scaled back down. Tasks running a single shard
  are unaffected. If you see this error, enable the `scale_out` feature
  flag (requires runtime v2) or scale the task back to a single shard.
