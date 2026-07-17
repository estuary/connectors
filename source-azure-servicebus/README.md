# source-azure-servicebus — native Estuary capture connector

A **pull** connector for Azure Service Bus, written against `estuary-cdk`. Estuary
runs it on a schedule (default every 15 min), so it replaces the Azure
**Function** (Service Bus trigger → HTTP-ingest) bridge in
`../../azure_servicebus_estuary` — **no Azure Function, no per-execution cost, no VM.**

## How it works

```
Service Bus queue/subscription
  └─ fetch_changes (every 15m via interval): open async receiver, drain in batches
       ├─ receive_messages (peek-lock)
       ├─ yield ServiceBusLog docs        ──▶ collection (key /sequence_number)
       ├─ yield checkpoint cursor          ──▶ Estuary persists docs durably
       └─ complete_message(...)  ← settle ONLY after the checkpoint
     loop until the entity is drained, then exit (Estuary re-invokes in ~15m)
```

**Correctness (at-least-once):** each batch is `complete`d (removed from the
broker) *only after* Estuary has checkpointed its documents. A crash between
checkpoint and settle redelivers the batch; the `/sequence_number` key upserts
the duplicates away. No message is acked before its data is durable.

**Scheduling:** `ResourceConfig.interval = 15m`. When messages are flowing, batches
re-invoke immediately (draining continuously); when the entity is empty, the
runtime waits ~15m before the next sweep.

## Config

| Field | Purpose |
|---|---|
| `connection_string` | Namespace-level SB connection string (no `;EntityPath=`) |
| `queue_name` | Read a queue — set this **OR** topic+subscription |
| `topic_name` + `subscription_name` | Read a topic subscription |
| `advanced.max_messages_per_sweep` | Cap per invocation (default 5000) |
| `advanced.max_wait_seconds` | Idle timeout that marks the entity drained (default 5) |
| `advanced.prefetch_count` | Messages per receive call (default 250) |

## Document shape

```json
{
  "sequence_number": 42,
  "message_id": "...",
  "enqueued_time": "2026-07-09T10:00:00+00:00",
  "subject": "...", "content_type": "...", "correlation_id": "...",
  "application_properties": { ... },
  "body": { ... }          // JSON parsed; plain text -> {"raw": "..."}
}
```
Same envelope fields as the old Function bridge; keyed on `sequence_number`
(unique & monotonic within an entity) instead of an Estuary-assigned UUID, so the
connector is idempotent across redeliveries.

## Operational notes
- **Peek-lock duration:** each batch must be emitted + checkpointed + settled
  within the entity's lock duration (default 60s, max 5m). Keep `prefetch_count`
  modest so a batch settles well inside the lock window.
- **One binding = one entity.** `/sequence_number` is unique within a single
  queue/subscription; point one capture at one entity (or add a binding per
  entity, each to its own collection).
- **Dead-lettering** is unchanged — messages that repeatedly fail delivery still
  land in the entity's DLQ per your Service Bus policy.

## Local test / build
Same flow as any `estuary-cdk` connector:
```
poetry -C source-azure-servicebus install       # python 3.12
flowctl preview --source source-azure-servicebus/test.flow.yaml   # needs real SB creds
```
Build via the shared `estuary-cdk/common.Dockerfile`
(`--build-arg CONNECTOR_NAME=source-azure-servicebus CONNECTOR_TYPE=capture`).
