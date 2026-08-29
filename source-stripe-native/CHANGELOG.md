# Changelog

## 2026-08-31

### Fixed
- Memory no longer grows without bound while capturing from connected accounts. Every
  per-account subtask registered a permanently-retained logger, leaking roughly 0.6 KB
  per subtask until the connector exceeded its memory limit. Subtask names and the log
  `source` field no longer embed the account id; it is carried in the `subtask_id` log
  field instead.

## 2026-08-20

### Changed
- Backfilled `Events` documents are now marked as creations (`_meta/op: c`) instead of
  updates (`u`).

### Fixed
- `Events` documents captured by incremental replication now include every field Stripe
  returns. `request`, `livemode`, `pending_webhooks`, `account`, and `context` were
  previously dropped, so the same event looked different depending on whether it was
  captured live or by a backfill. Documents captured before this change are not rewritten.
- Live-captured `Events` documents no longer carry a null `data.previous_attributes` for
  event types where Stripe omits the field entirely.
