---
name: classify-stream-types
description: Choose each endpoint's replication strategy (webhook, incremental + backfill, incremental-only, snapshot, scheduled backfill) for an estuary-cdk connector. Use when adding or planning streams; the caller implements the result.
argument-hint: "[connector-name]"
allowed-tools: Bash Read Grep Glob WebFetch WebSearch
---

Classify each endpoint of `source-$1` into a replication strategy, with a rationale. The output is the classification; the caller owns confirmation and implementation. Read the connector's existing streams first so the choice matches local convention, then research the provider's docs for each endpoint's filtering, pagination and cursor capabilities.

## Decision flowchart

This chart chooses a **strategy**. Implementation rules for fetch functions (window semantics, resume keys, checkpointing) are in the `fetch-function-rules` skill; the `created_at` and LogCursor criteria below appear there as `FETCH-CURSOR-MUST-BE-UPDATED` and `FETCH-LOGCURSOR-AFTER-DOCS`. Keep the two in sync if either changes.

Evaluate each endpoint in order:

1. **Does the provider push events via HTTP?** → **Webhook** stream (`WebhookCaptureSpec`), set up per `create-webhook-connector`.
2. **Does the endpoint filter by date range or a cursor that persists over time** (`updated_at`, monotonic id, event timestamp, sequence number; not `created_at` alone)? → **Incremental + Backfill**. Sorting support is a bonus, not a requirement.
3. **No filtering, but a cursor field and reverse sort?** → **Incremental only**. `fetch_changes` walks from the newest document back to the cursor (initially `start_date`) and yields a checkpoint only once the walk reaches it; an interrupted walk restarts from the top.
4. **Small dataset, no change tracking?** → **Snapshot**. Snapshots also infer deletions.
5. **Mutable resource with only a `created` cursor?** Incremental on `created` catches new rows and silently misses every update and delete. Pair it with a **scheduled backfill** (below).
6. **Large dataset, no filtering or sorting?** → Look for another endpoint; failing that, `GATE-STRATEGY-UNCLEAR`.

`GATE-INCREMENTAL-ONLY` ([`interaction-mode.md`](../../shared/interaction-mode.md)): any usable cursor prefers incremental + backfill. Human-in-the-loop: incremental-only needs the user's explicit word that backfill is unneeded. Autonomous: any usable cursor → incremental + backfill; none → snapshot, which is slower but observes every row; ledger the tradeoff and the size estimate.

## Scheduled backfill as update fetcher

When a resource is mutable and the provider offers no way to target its updates (no events, no `updated_at` filter), a periodic full re-list on a cron is the correct answer. The destination dedupes by `id`, so re-emitting unchanged rows is harmless; the goal is to eventually observe updates.

Rule out the alternatives first so the choice is deliberate:

- **Webhook or event-stream polling**: only if the provider emits create/update/delete events for this resource. Verify in the docs and, where possible, empirically; event catalogs omit events providers really send.
- **Lookback window** (`source-hubspot-native`, `source-outreach`, `source-jira-native`): a second incremental subtask trailing the realtime cursor by a fixed lag (1h, 6h) recovers late-arriving rows from an eventually-consistent index.
- **Sliding window of recent data** (`source-calendly`, scheduled events): re-fetch a bounded window like `[now − N months, now + M months]` each poll, using a server-side time filter that correlates with where updates happen (`min_start_time` for upcoming meetings). Fails for resources whose historical records can be edited at any time.

If none fit → `GATE-STRATEGY-UNCLEAR`. Human-in-the-loop: ask for assessment. Autonomous: small dataset → snapshot; large with any usable cursor (a `created`-only cursor counts) → incremental + scheduled backfill, `fetch_changes` on the cursor for prompt new rows and `fetch_page` on the cron for updates and deletes; large with no cursor → snapshot on a long interval. A bare `fetch_page`-only cron is never the answer. Ledger the size estimate, the cursor decision and the alternatives ruled out as an open question.

**Wiring.** Add the stream to the connector's scheduled-backfill list and pass `schedule=DEFAULT_SCHEDULE` in its `ResourceConfigWithSchedule`; streams without a schedule omit the field, whose default is `""`. Reference: `SCHEDULED_BACKFILL_STREAMS` in `source-stripe-native/source_stripe_native/models.py` and `DEFAULT_SCHEDULE` in its `resources.py`.

## Reference implementations

Copy the shape from the reference, not from memory:

| Strategy               | Reference                                                                                                                                                                                |
| ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Incremental + backfill | `source-sentry/source_sentry/resources.py`, `open_issue_binding`. Initial state sets both `inc` (cursor = cutoff) and `backfill` (cutoff, `next_page=None` meaning "start of backfill"). |
| Incremental only       | `source-front/source_front/resources.py`, `incremental_resources_with_cursor_fields`. Initial state sets `inc` with cursor = `start_date`.                                               |
| Snapshot               | `source-ashby/source_ashby/resources.py`.                                                                                                                                                |
| Webhook                | `create-webhook-connector`.                                                                                                                                                              |

**Snapshots use `SnapshotResource`, never the generic `Resource`, with its defaults** (no `model`, `key`, `initial_state` or `schema_inference`). The model's schema is the collection's write schema, and every required field must be present on every document written, including the tombstones the CDK emits for rows that vanished between snapshots. A tombstone missing a required field fails the capture with a schema violation. `SnapshotResource` defaults model and tombstone to `BaseDocument` so they always agree; with the default `/_meta/row_id` key that is sufficient.

## Backfill shape

- `fetch_page(log, page_cursor, cutoff)` walks history from oldest to `cutoff`, one page or time window per invocation, yielding the `PageCursor` for the next.
- The `PageCursor` carries progress across the CDK's 24-hour restart. What it may encode, and when to yield it, is `fetch-function-rules` (`FETCH-CHECKPOINT-STABLE-STATE`, `FETCH-VALUE-WATERMARK-RESUME`, `FETCH-DICT-CURSOR-WORKLIST`).
- Return without yielding a `PageCursor` to signal completion.
- `cutoff` is where incremental takes over; documents at or after it are suppressed (`FETCH-SEAM-PRECISION`).
