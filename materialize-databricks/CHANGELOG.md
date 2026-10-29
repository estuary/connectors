# Changelog

## 2026-10-29
### Changed
- A table's load switches to two phases (see `two_phase_loads`) after a load
  of it takes at least 20 seconds, and keeps them while they're faster than
  that load. When they're slower, its loads go back to one phase, and try two
  again after 10 more slow loads. The `no_adaptive_two_phase_loads` feature
  flag turns this off, and `two_phase_loads` still runs every load in two
  phases.
- A MERGE of fewer than 100,000 rows reads them straight from the staged
  files, instead of first copying them into a staging table. This saves a
  query of a few seconds from each commit of small batches.

## 2026-10-28
### Fixed
- A load no longer fails, restarting the task, when Databricks rate-limits
  the download of its results (`cloud fetch batch loader failed to download
  results: HTTP error 429`). Downloads that fail with a transient error are
  now retried with backoff, honoring the `Retry-After` header.

### Changed
- With `two_phase_loads`, a load restricts its read of documents to the files
  holding its matching rows for up to 10,000 matched files, instead of 5,000.
  Loads matching more files read every file within their key bounds.

## 2026-10-27
### Added
- Load, staging and commit queries that run for at least five seconds are
  logged with the metrics Databricks records of them in its query history:
  time spent queued, compiling, executing and returning results, and the
  files and bytes they read and pruned. Load and merge queries also log the
  key bounds they were restricted to. The metrics are fetched in the
  background, without holding up the transaction.
- A `two_phase_loads` feature flag, off by default. With it, each table's
  load first finds the files holding rows that match its keys, reading only
  the key columns, then reads documents from those files alone. Both queries
  read the same version of the table. Loads whose matching rows are
  concentrated in a few of the files they would otherwise read get faster,
  and a load that matches no rows skips reading documents altogether.

## 2026-10-26
### Changed
- A transaction's tables are loaded with one query each, up to five at a time,
  instead of in a single query.
- Load and merge queries on tables with a `TIMESTAMP` key column split the
  key range at wide gaps between the transaction's keys, so a few keys far
  from the rest no longer widen the scan to everything in between.

## 2026-10-22

### Added
- The connector status on the task page now shows what the materialization is
  doing, such as loading, staging or committing data, or waiting for its next
  scheduled sync.

## 2026-10-13
### Changed
- Load and merge queries on tables with a date-time key column now restrict
  the scan to each transaction's key range, exact for `TIMESTAMP` keys and
  by date for `STRING` keys.

## 2026-10-12
### Fixed
- A materialization that restarts partway through a commit no longer fails on
  every restart after it when a table's staging data was already cleaned up.

## 2026-10-07
### Changed
- A transaction's tables are committed concurrently, up to five at a time,
  instead of one after another.

## 2026-10-01
### Changed
- Updates to existing rows are copied into a Delta staging table once a
  table's rows are staged, and the commit's MERGE reads that table instead of
  the staged JSON files. In transactions with several tables, this loading
  overlaps with staging the remaining tables.

## 2026-09-29
### Fixed
- Each table's staged files are finished and uploaded as soon as the
  transaction's rows move on to the next table, instead of every table's file
  staying open until commit. Wide transactions no longer hold a compressor per
  table, and uploads overlap with writing the remaining tables.

## 2026-09-28
### Fixed
- Materializations using `OAuth2 M2M` credentials no longer fail every
  transaction with `pat auth: token is required`. Since 2026-09-23 the staging
  directory was created with a client that always used PAT authentication,
  regardless of the configured `auth_type`.

## 2026-09-23
### Changed
- Each transaction's staged files are uploaded into a directory of their own,
  and the load and MERGE queries read that directory as one relation with the
  files' schema given explicitly, instead of one schema-inferred scan per
  file. COPY INTO no longer infers column types either. Files staged by an
  earlier version and still pending in the checkpoint commit as before.

## 2026-09-17
### Fixed
- Fixed escaping of identifiers containing backslash character.

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
