---
name: add-stream
description: Add a new stream to an existing estuary-cdk connector — classify the endpoint, register it, regenerate flow discovery, refresh snapshots. Use when extending a `source-*` connector with another endpoint.
argument-hint: "[connector-name] [stream-name]"
allowed-tools: Bash Read Write Edit Glob Grep WebFetch WebSearch
---

Add a `stream-name` stream to the `source-$1` connector. Read a few neighboring streams in the same connector before designing the new one — local conventions outrank any generic pattern.

## Laws

These apply in every phase. Re-read them before each phase boundary.

**Shared laws** — read both before Phase 0; they are the single authority:

- [`.claude/shared/session-conduct.md`](../../shared/session-conduct.md)
- [`.claude/shared/provider-api-consent.md`](../../shared/provider-api-consent.md)

**Skill-specific laws:**

1. Phase 1 (Rate Limit Survey) is what sets the budget referenced by `API-BUDGET-20RPH`. Don't enter Phase 4 without it.
2. Prefer snapshot tests over fine-grained assertions when checking outputs.

## Phase 0 — Reconnoiter

Confirm the connector exists. Locate its `models.py`, `resources.py`, `api.py` (or equivalents — naming varies between Python connectors). Read streams in the same connector before designing anything.

**Clean-room check.** Kick off the connector's existing test suite (typically `pytest` from the connector directory) in the background as soon as Phase 0 begins. Phases 1–3 are read-only / planning and may proceed in parallel while the suite runs — do **not** block on it. The clean-room result is a **gate on Phase 4**: before writing any code for the new stream, confirm the baseline passes so later failures are attributable to the new stream, not pre-existing drift.

If the suite fails:

- **Simple schema drift** (the connector spec/discover output has shifted but the code is unchanged): you may dispatch the `regenerate-flow-discovery` agent to refresh the snapshots, then commit the result on its own with the message `<connector-name>: update tests`. Keep this commit separate from the new-stream work so the PR diff stays scoped.
- **Flakey fields in the diff** (timestamps like `updated_at`, ETags, anything that changes between runs): surface them to the user and ask whether to add them to the connector's `FIELDS_TO_REDACT` list in `tests/test_snapshots.py` (name varies between connectors — grep for `FIELDS_TO_REDACT` to find the convention).
- **Anything else:** stop and surface the failure to the user before entering Phase 4.

## Phase 1 — Rate Limit Survey

Before researching the requested stream, figure out the provider's API rate limits.

**Check for cached notes first.** The Bruno collection is the home for API knowledge: verified limits and throttling behavior live in `<connector>/bruno/opencollection.yml`'s `docs:` block under `## API constraints (account-wide)`. If the section exists, use those values — don't re-derive.

**If the section (or the collection) is absent**, look up the limits:

- The published limit for the endpoint family (typically requests/second or requests/hour, sometimes per-key, sometimes per-account).
- Any _tighter_ limits that apply to the specific endpoints this stream will touch — search endpoints, expensive list endpoints, and event firehoses often have lower budgets than the general bucket.
- Any soft-quota / overage / 429 backoff behavior the provider documents.

Then **record the findings in the collection** so the next stream addition doesn't repeat the work. The block format — and where endpoint-specific limitations go instead (a `**LIMITATION**` finding on the proving request) — is defined in `bruno-probe-endpoint`, which also stands the collection up during verification; if it doesn't exist yet at this point, carry the findings forward and write them there once it's created.

**Set the budget for the rest of the skill** (`API-BUDGET-20RPH`, `API-DISCOVER-FREE`, `API-COST-SAVER-DISABLE` in [`.claude/shared/provider-api-consent.md`](../../shared/provider-api-consent.md)):

- **All required endpoints > 20 req/hr:** run `flowctl preview` / `pytest` / captures freely.
- **Any required endpoint ≤ 20 req/hr:** ask before each run; plan to disable unrelated bindings in `test.flow.yaml` (`disable: true`) before testing.
- **`flowctl raw discover` is always allowed**, regardless of budget.

## Phase 2 — Endpoint Survey

State _every_ viable provider endpoint that could back this stream (list, search, expand, related event streams, etc.). For each, summarize what it offers and what it lacks (filterability, sorting, cursors, freshness guarantees, regional availability).

Pick one and explain why it fits the connector's constraints. **Call out when you're deliberately following an existing convention** in this connector — e.g. "every other stream here uses list + events polling, sticking with that." Conversely, if you're deviating, name the deviation and justify it.

Surface rejected alternatives with a one-line "why not" each. This becomes useful context for reviewers later.

**Document grain.** Decide here — with the endpoint, before classification — what one document _is_. The document is the atomic fact, not the API's transport grouping: list endpoints often shape their repeated element as a container (a member row wrapping an `activity[]` of independently-identified events, a report wrapping a `timeseries[]`). When the repeated element contains its own array of items carrying their own identity (action, timestamp, id), yield the **inner item**, with the wrapper's identifying fields denormalized into it (or into `_meta`) — never the wrapper with the array still nested. Tells that you've picked the container:

- The collection key can't be unique without reaching _inside_ a nested array (`[campaign_id, email_id]` doesn't identify an event; `[campaign_id, email_id, action, timestamp]` does — so the event is the document).
- Re-capturing the entity would _mutate a growing array_ instead of appending new documents. Under last-write-wins reduction, a later partial fetch (e.g. one filtered by `since`) silently **erases** previously captured history from the nested array.
- The wrapper is a _view over_ facts (an aggregate/rollup), while the inner items are the facts themselves — immutable once they happen.

The decision test: "what is the thing that, once true, never changes?" That's the document. Aggregates change; events don't.

## Phase 3 — Classification

Defer to the `classify-stream-types` skill. Don't re-derive its flowchart here. Bring back: the chosen replication strategy (webhook, incremental+backfill, incremental-only, or snapshot) and the rationale.

**Checkpoint:** present the chosen endpoint (from Phase 2), the document grain, the classification, and the rationale to the user, and ask whether they want to proceed to Phase 4 (Model Implementation). Stop here until they say yes. This is the natural break for the user to course-correct before any connector code gets written.

## Phase 4 — Model Implementation

Find the closest existing stream in _the same connector_ whose replication strategy matches the one chosen in Phase 3. Replicate its pattern verbatim and cite the exact `file:line` you copied from. Do not introduce a new pattern when the connector already has one — if the existing pattern doesn't fit, that's a Phase 2 conversation, not a Phase 4 one.

For incremental and backfill streams, **invoke the `fetch-function-rules` skill before writing the fetch functions.** It holds the window semantics, resume-key, and checkpointing rules — four of them are data-loss-class and none are inferable from the sibling stream you're copying. Cite rule IDs (`FETCH-SEAM-PRECISION`, …) when you apply or deviate from them. Do not mention rule IDs in code.

If the stream is a child entity requiring a parent ID to list, use the `child-entities` skill for the per-parent cursor and context-injection pattern.

## Phase 5 — Registration

Add the new stream wherever the connector enumerates streams. That's often a top-level list, a discovery function, or both — grep the connector for how other streams declare themselves and match the form.

Check whether any per-connector "special lists" apply (split-child, regional, scheduled-backfill, exempt-from-X). The way to know: grep for sibling streams that share traits with the new one, and see which lists they appear in.

## Phase 5.5 — Static checks & formatting

After the implementation compiles and before regenerating flow discovery, run over the connector package:

1. **Type-check** with `pyright` (or `basedpyright`), inside the connector's poetry env so imports resolve: `poetry run basedpyright source_<pkg>/`.
   - Fix every error in code this session wrote.
   - **Read the warnings individually too — never dismiss the warning list wholesale.** Actionable classes hide there at warning severity: `reportDeprecated` (e.g. `typing.AsyncGenerator` → `collections.abc`) and `reportPrivateImportUsage` (importing a symbol from a module that re-imports but doesn't re-export it — follow the "Import from X instead" hint). Fix these in session-written code; only generics-inference noise from CDK internals may be left.
   - House-pattern errors (e.g. `request_class`/`spec`/`credentials_title` override complaints inherited from the scaffold/reference idioms) may be pre-existing noise: **verify parity by running the same check on a sibling connector** before ignoring them, and say so out loud.
2. **Organize imports**: `ruff check --select I --fix source_<pkg>/ tests/`.
3. **Format**: `black source_<pkg>/ tests/`.

Re-run the test suite if any of these changed code. If a tool isn't on PATH, find it (editor tooling dirs count) or ask — don't skip the step silently.

## Phase 6 — Flow Regeneration

Dispatch the `regenerate-flow-discovery` agent (runs on Haiku in its own context) via the Agent tool. Pass it the connector name and — since Phase 1 already surveyed the provider's rate limits — whether the budget is cleared (all required endpoints > 20 req/hr) so it can run the snapshot tests without re-asking for consent.

## Phase 7 — Snapshot Refresh

This happens inside the `regenerate-flow-discovery` agent. Verify when it returns:

- The new stream appears in the discover snapshot.
- The capture snapshot may or may not contain the new stream. That's expected and usually matches existing comments in the test file for similarly-quiet streams.
- `git diff --stat` is scoped to the new stream. If unrelated schemas changed (uniform per-file deltas across many files), that's a CDK schema sweep — recommend the user split it into its own commit so the PR diff stays scoped.

## Reference connectors

For canonical examples of stream patterns at different replication strategies, look at:

- `source-sentry/` — incremental + backfill via cursor filtering on the list endpoint.
- `source-front/` — incremental only. Also the simplest overall REST skeleton.
- `source-ashby/` — snapshot.
- `source-posthog/` — incremental child entities under a parent (see also the `child-entities` skill).
- `source-airtable-native/` — pagination and nested resources.
- `source-appsflyer/` — webhook + pull hybrid in one connector (see `create-webhook-connector`).

## Out of scope

- Adding webhook-receiver streams. Use `create-webhook-connector` instead.
