# Changelog

## 2026-08-14

### Fixed
- The Messages stream no longer filters re-fetched records against the
  `date_sent` cursor before emitting them. Previously, records returned by
  the lookback window with both `date_sent` and `date_updated` older than
  the persisted cursor were silently dropped, so in-place updates (e.g.
  `price` populated shortly after a message is sent) never reached the
  collection and messages missed on first read were never captured.
- Messages with a null `date_sent` (queued or failed messages) no longer
  cause the stream to fail; they are emitted as-is and do not advance the
  cursor.

### Changed
- The Messages stream emits records as they are paged from the API instead
  of buffering and sorting an entire date slice in memory. The cursor
  advances only after a slice is read to completion, so checkpoints taken
  mid-slice cannot skip past unprocessed records.
