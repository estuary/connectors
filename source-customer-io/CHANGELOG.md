# Changelog

## 2026-09-21

### Added
- Initial release of the Customer.io capture connector.
- `messages` resource, capturing message deliveries incrementally with a scheduled backfill. Deliveries have no updated-at field and cannot be filtered by engagement time, so a trailing re-scan pass (`advanced.metrics_rescan_window`, default 7 days) re-reads them once their engagement metrics have settled.
- Full refresh resources for configuration objects: `campaigns`, `broadcasts`, `segments`, `transactional_messages`, `subscription_topics`, `object_types`, `collections`, `newsletters`, `sender_identities` and `optouts`. None of these endpoints offers a time filter, so each is re-read in full and deletions are detected by comparison.
- `optouts` polls hourly rather than every five minutes: it is the only resource whose size scales with the profile base rather than with how much a workspace has been configured.
- Incremental resources for Design Studio: `design_studio_emails`, `design_studio_components` and `design_studio_folders`. These are the only Customer.io endpoints that can be filtered by when a row last changed, so they sync on `updated` with a scheduled backfill rather than being re-read in full. Note that their filter bounds are exclusive, the mirror of `/v1/messages`.

### Known limitations
- `/v1/messages` serves at most six months of history. A `start_date` older than that is clamped, with a warning.
- Engagement metrics carry one timestamp per metric type, so repeat opens and clicks of the same delivery are not individually captured. Customer.io's reporting webhooks are the only source of per-event resolution.
- Drafted deliveries are not captured. Customer.io's `drafts` flag swaps the returned population rather than adding to it, and offers no combined view, so drafts would need a separate resource with an independent cursor.
- Deletions are not detected for the Design Studio resources. They sync incrementally, so a deleted template is never revisited and remains in the collection. The full refresh resources do detect deletions.
- `design_studio_emails` captures template metadata rather than rendered content.
- ESP suppression lists are not captured. That endpoint pages by offset rather than by continuation token and fans out over a path parameter, so it needs its own design.
