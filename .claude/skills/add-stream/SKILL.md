---
name: add-stream
description: Add a new stream to an existing estuary-cdk connector — classify the endpoint, register it, regenerate flow discovery, refresh snapshots. Use when extending a `source-*` connector with another endpoint.
argument-hint: "[connector-name] [stream-name]"
allowed-tools: Bash Read Write Edit Glob Grep WebFetch WebSearch
---

Add a `stream-name` stream to the `source-$1` connector. Read a few neighboring streams in the same connector before designing the new one — local conventions outrank any generic pattern.

## Laws

These apply in every phase. Re-read them before each phase boundary.

1. Open the session with a TODO list. One task per phase below, plus any provider-specific work that comes up.
2. **Budget rule for captures and previews:** Phase 1 (Rate Limit Survey) sets the budget. If every endpoint this stream and its siblings will touch allows **more than 20 requests/hour**, you may run `flowctl preview`, `pytest` (including the snapshot tests that drive `flowctl preview` under the hood), and capture-running commands as often as needed without further consent. If any required endpoint is tighter than that, ask before each run, and consider Law #6.
3. **Cost-saver:** `disable: true` on a binding in `test.flow.yaml` skips that stream during preview/capture. When the budget is tight or you only care about the new stream's behavior, disable unrelated bindings before running tests. Restore them before committing.
4. **`flowctl raw discover` is always free.** It exercises only the connector's discovery path and doesn't burn provider API budget. No consent needed.
5. Prefer snapshot tests over fine-grained assertions when checking outputs.
6. State out loud when you're deliberately following an existing convention. State equally out loud when you're deviating, and justify it. Silence is worse than confirmation.
7. When you copy a pattern from another stream, cite the exact `file:line` you copied from so the user can sanity-check.
8. If you propose a function/field/flag that you remember seeing in the connector, grep for it before recommending — the codebase may have changed since memory was formed.

## Phase 0 — Reconnoiter

Confirm the connector exists. Locate its `models.py`, `resources.py`, `api.py` (or equivalents — naming varies between Python connectors). Read streams in the same connector before designing anything.

**Clean-room check.** Kick off the connector's existing test suite (typically `pytest` from the connector directory) in the background as soon as Phase 0 begins. Phases 1–3 are read-only / planning and may proceed in parallel while the suite runs — do **not** block on it. The clean-room result is a **gate on Phase 4**: before writing any code for the new stream, confirm the baseline passes so later failures are attributable to the new stream, not pre-existing drift.

If the suite fails:

- **Simple schema drift** (the connector spec/discover output has shifted but the code is unchanged): you may run the `regenerate-flow-discovery` sub-skill to refresh the snapshots, then commit the result on its own with the message `<connector-name>: update tests`. Keep this commit separate from the new-stream work so the PR diff stays scoped.
- **Flakey fields in the diff** (timestamps like `updated_at`, ETags, anything that changes between runs): surface them to the user and ask whether to add them to the connector's `FIELDS_TO_REDACT` list in `tests/test_snapshots.py` (name varies between connectors — grep for `FIELDS_TO_REDACT` to find the convention).
- **Anything else:** stop and surface the failure to the user before entering Phase 4.

## Phase 1 — Rate Limit Survey

Before researching the requested stream, figure out the provider's API rate limits.

**First, check for cached notes.** Read `<connector>/CLAUDE.md` (create the file if it doesn't exist). If it has an "API Rate Limits" section, use those values — don't re-derive. They're authoritative until proven stale (and you'll only spot staleness if a run starts 429-ing).

**If the section is absent**, look up the limits:

- The published limit for the endpoint family (typically requests/second or requests/hour, sometimes per-key, sometimes per-account).
- Any _tighter_ limits that apply to the specific endpoints this stream will touch — search endpoints, expensive list endpoints, and event firehoses often have lower budgets than the general bucket.
- Any soft-quota / overage / 429 backoff behavior the provider documents.

Then **write the findings to `<connector>/CLAUDE.md`** under an `## API Rate Limits` section so the next stream addition doesn't repeat the work.

```markdown
## API Rate Limits

- General: <N> req/sec per key (per provider docs, link).
- Tighter buckets:
  - `/v1/<endpoint>`: <M> req/min
  - `/v1/<events-firehose>`: <K> req/hr
- 429 behavior: <retry-after / exponential backoff / hard-cut>.
```

**Set the budget for the rest of the skill**:

- **All required endpoints > 20 req/hr:** run `flowctl preview` / `pytest` / captures freely.
- **Any required endpoint ≤ 20 req/hr:** ask before each run; plan to disable unrelated bindings in `test.flow.yaml` (`disable: true`) before testing.
- **`flowctl raw discover` is always allowed**, regardless of budget.

## Phase 2 — Endpoint Survey

State _every_ viable provider endpoint that could back this stream (list, search, expand, related event streams, etc.). For each, summarize what it offers and what it lacks (filterability, sorting, cursors, freshness guarantees, regional availability).

Pick one and explain why it fits the connector's constraints. **Call out when you're deliberately following an existing convention** in this connector — e.g. "every other stream here uses list + events polling, sticking with that." Conversely, if you're deviating, name the deviation and justify it.

Surface rejected alternatives with a one-line "why not" each. This becomes useful context for reviewers later.

## Phase 3 — Classification

Defer to the `classify-stream-types` skill. Don't re-derive its flowchart here. Bring back: the chosen replication strategy (webhook, incremental+backfill, incremental-only, or snapshot) and the rationale.

**Checkpoint:** present the chosen endpoint (from Phase 2), the classification, and the rationale to the user, and ask whether they want to proceed to Phase 4 (Model Implementation). Stop here until they say yes. This is the natural break for the user to course-correct before any connector code gets written.

## Phase 4 — Model Implementation

Find the closest existing stream in _the same connector_ whose replication strategy matches the one chosen in Phase 3. Replicate its pattern verbatim and cite the exact `file:line` you copied from. Do not introduce a new pattern when the connector already has one — if the existing pattern doesn't fit, that's a Phase 2 conversation, not a Phase 4 one.

## Phase 5 — Registration

Add the new stream wherever the connector enumerates streams. That's often a top-level list, a discovery function, or both — grep the connector for how other streams declare themselves and match the form.

Check whether any per-connector "special lists" apply (split-child, regional, scheduled-backfill, exempt-from-X). The way to know: grep for sibling streams that share traits with the new one, and see which lists they appear in.

## Phase 6 — Flow Regeneration

Defer to the `regenerate-flow-discovery` sub-skill.

## Phase 7 — Snapshot Refresh

This happens inside `regenerate-flow-discovery`. Verify when you come back:

- The new stream appears in the discover snapshot.
- The capture snapshot may or may not contain the new stream. That's expected and usually matches existing comments in the test file for similarly-quiet streams.
- `git diff --stat` is scoped to the new stream. If unrelated schemas changed (uniform per-file deltas across many files), that's a CDK schema sweep — recommend the user split it into its own commit so the PR diff stays scoped.

## Reference connectors

For canonical examples of stream patterns at different replication strategies, look at:

- `source-sentry/` — incremental + backfill via cursor filtering on the list endpoint.
- `source-front/` — incremental only.
- `source-ashby/` — snapshot.
- `source-posthog/` — incremental child entities under a parent (see also the `child-entities` skill).

## Out of scope

- Adding webhook-receiver streams. Use `create-webhook-connector` instead.
