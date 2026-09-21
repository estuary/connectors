# Changelog

## 2026-09-21

### Added
- Initial release of the Customer.io capture connector.
- `messages` resource, capturing message deliveries incrementally with a scheduled backfill. Deliveries have no updated-at field and cannot be filtered by engagement time, so a trailing re-scan pass (`advanced.metrics_rescan_window`, default 7 days) re-reads them once their engagement metrics have settled.

### Known limitations
- `/v1/messages` serves at most six months of history. A `start_date` older than that is clamped, with a warning.
- Engagement metrics carry one timestamp per metric type, so repeat opens and clicks of the same delivery are not individually captured. Customer.io's reporting webhooks are the only source of per-event resolution.
- Drafted deliveries are not captured. Customer.io's `drafts` flag swaps the returned population rather than adding to it, and offers no combined view, so drafts would need a separate resource with an independent cursor.
