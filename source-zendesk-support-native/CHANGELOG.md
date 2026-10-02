# Changelog

## 2026-10-02

### Added
- New `side_conversation_events` stream captures every side conversation event, including the message body (`message.body` and `message.html_body`) of each email and reply. It's available when side conversations are enabled and the connector's credentials belong to an admin.

## 2026-10-01

### Changed
- `ticket_audits`, `ticket_comments`, and `side_conversations` now fetch up to 5 tickets' records at a time instead of one at a time, so these streams catch up faster after many tickets are updated at once.

## 2026-09-10

### Changed
- Incremental streams now trail roughly 5 minutes behind the present to combat the Zendesk Support API's eventually consistent behavior. Records still arrive on each binding's normal polling interval, with up to 5 minutes of additional delay.

## 2026-08-05

### Fixed
- `ticket_metric_events`, `ticket_skips`, and `ticket_activities` no longer drop the majority of records during incremental replication. Each poll discarded an entire page of results and often failed to advance its cursor, so these streams captured only a small fraction of new records once the initial backfill completed. Backfill was unaffected. Existing captures should backfill these bindings to recover the records that were missed.
