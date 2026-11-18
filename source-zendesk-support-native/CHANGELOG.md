# Changelog

## 2026-11-23

### Changed
- `organizations`, `users`, `tickets`, `ticket_audits`, `ticket_comments`, `side_conversations`, `ticket_metrics`, and `ticket_metric_events` read from Zendesk's list endpoints instead of its incremental exports, which Zendesk is ending for third-party apps. This applies to captures created on or after 2026-11-23, and to existing captures with the `use_list_endpoints` feature flag.
- On the list endpoints, deleted tickets and users arrive as deletion documents instead of `status: "deleted"` tickets and `active: false` users, about 1.5 hours after they're deleted.
- On the list endpoints, archived tickets and their child records aren't captured, organization deletes aren't captured, and `tickets` documents no longer include `fields`.
- On the list endpoints, `ticket_metric_events` arrive when their ticket changes and are emitted again on later changes.

## 2026-11-06

### Changed
- `calls` and `call_legs` are discovered only while Zendesk allows access to their incremental exports.

## 2026-10-23

### Fixed
- `tickets`, `ticket_metrics`, `ticket_audits`, `ticket_comments`, and `side_conversations` now include tickets handled entirely by Zendesk's AI agent.

## 2026-10-12

### Fixed
- `satisfaction_ratings`, `audit_logs`, `ticket_skips`, `ticket_metric_events`, `ticket_activities`, `organizations`, `calls`, and `call_legs` now wait to poll Zendesk until at least 1 minute of new data is available.

## 2026-10-08

### Changed
- `ticket_metric_events` now trails roughly 30 minutes behind the present. Previously, events recorded more than 5 minutes late could be missed.

## 2026-10-01

### Changed
- `ticket_audits`, `ticket_comments`, and `side_conversations` now fetch up to 5 tickets' records at a time instead of one at a time, so these streams catch up faster after many tickets are updated at once.

## 2026-09-10

### Changed
- Incremental streams now trail roughly 5 minutes behind the present to combat the Zendesk Support API's eventually consistent behavior. Records still arrive on each binding's normal polling interval, with up to 5 minutes of additional delay.

## 2026-08-05

### Fixed
- `ticket_metric_events`, `ticket_skips`, and `ticket_activities` no longer drop the majority of records during incremental replication. Each poll discarded an entire page of results and often failed to advance its cursor, so these streams captured only a small fraction of new records once the initial backfill completed. Backfill was unaffected. Existing captures should backfill these bindings to recover the records that were missed.
