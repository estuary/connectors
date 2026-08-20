# Changelog

## 2026-08-20

### Fixed
- `Events` documents captured by incremental replication now include every field Stripe
  returns. `request`, `livemode`, `pending_webhooks`, `account`, and `context` were
  previously dropped, so the same event looked different depending on whether it was
  captured live or by a backfill. Documents captured before this change are not rewritten.
- Live-captured `Events` documents no longer carry a null `data.previous_attributes` for
  event types where Stripe omits the field entirely.
