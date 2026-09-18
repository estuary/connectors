# source-linear

Captures Issues, Projects, Initiatives and Labels from [Linear](https://linear.app) via its
GraphQL API.

## Configuration

| Field | Description |
|---|---|
| `credentials.access_token` | Linear personal API key. Create one under **Settings → Security & access → Personal API keys**. Read access is sufficient. |
| `start_date` | Lower bound for the historical backfill. Defaults to 30 days before the connector first runs. |

Linear expects the key as a bare `Authorization: <key>` header with no `Bearer` prefix.

## Streams

| Stream | Root query | Mode | Cursor |
|---|---|---|---|
| `issues` | `issues` | Incremental + backfill | `updatedAt`, plus a second pass on `archivedAt` |
| `projects` | `projects` | Incremental + backfill | `updatedAt` |
| `initiatives` | `initiatives` | Incremental + backfill | `updatedAt` |
| `labels` | `issueLabels` | Incremental + backfill | `updatedAt` |

All four are keyed on `/id`. Only primary keys, cursors and the archival tombstone are
declared on the document models; every other field arrives through schema inference.

## Limitations

These are behaviors that will otherwise be discovered as data corruption. Read them before
relying on this connector for anything derived from deletions.

### 1. Archival and deletion of Projects, Initiatives and Labels is NOT captured

Archiving a record in Linear does **not** advance its `updatedAt`, so an archived record is
invisible to an `updatedAt`-cursored sweep. The usual escape — filtering on `archivedAt`
directly — is unavailable for these three streams: `ProjectFilter`, `InitiativeFilter` and
`IssueLabelFilter` expose no `archivedAt` comparator (only `IssueFilter` does).

**Consequence:** a Project, Initiative or Label archived or deleted in Linear remains in the
destination indefinitely, with `archivedAt: null`, indistinguishable from a live record.
Re-running a full backfill is the only way to reconcile.

If deletion fidelity matters for these streams, converting them to snapshot streams is the
cheapest fix — the shared fetch engine already produces the full walk a snapshot needs, so
it is a resource-registration change rather than new fetch logic.

### 2. Issues archival IS captured — but read the tombstone, not the cursor

`issues` runs a second pass filtered on `archivedAt`, so archived issues do arrive.
Downstream, treat `archivedAt != null` as the tombstone. Do **not** infer archival from
`updatedAt` movement: it does not change when a record is archived.

### 3. `identifier` is not captured for Projects or Initiatives

The field sits behind Linear's paid *Project IDs* / *Initiative IDs* add-ons. In GraphQL an
un-entitled field does not come back absent — it raises an error on every page — so
requesting it would degrade every response for workspaces without those add-ons. Use `id`,
`slugId` or `url` instead. `Issue.identifier` is core and is captured.

### 4. Initiatives require a paid Linear plan

Workspaces without the entitlement will see the stream discovered but permanently empty.

## Rate limits

Linear meters two independent hourly budgets per user, and **requests** is the binding one:

| Budget | Limit |
|---|---|
| Requests | 2,500 / hour |
| Complexity | 3,000,000 points / hour |
| Single query | 10,000 points (hard cap, rejected not clamped) |

The connector reads `X-RateLimit-Requests-Remaining` off every response and sleeps until the
window resets before the budget is exhausted. This matters because Linear signals
rate-limiting as **HTTP 400 with a `RATELIMITED` error code, not HTTP 429** — the CDK treats
4xx as terminal, so such a response cannot be retried by the framework. A backstop retry
exists for the race, but staying under the limit is the real defence.

Two related quirks worth knowing when reading logs:

- **HTTP 200 does not mean success.** Per the GraphQL spec, Linear returns 200 with a
  top-level `errors` array for query-level failures, and `data` may be populated alongside
  it. The connector treats populated-`data`-with-errors as a partial success: rows are
  emitted and the errors logged loudly.
- **Rate-limit reset headers are UTC epoch milliseconds**, not seconds and not a delta.

Maximum page size is 250 across all four streams; `first: 251` is rejected outright.

## Development

```bash
poetry install
poetry run pytest                  # snapshot tests
poetry run pytest --insta=update   # refresh snapshots
```

`bruno/` holds the live-verified request collection used to establish the API's behavior,
including the probes backing every claim above. Account-wide constraints are documented in
`bruno/collection.bru`; per-endpoint constraints sit on the request that proves them.
