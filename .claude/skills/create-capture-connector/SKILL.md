---
name: create-capture-connector
description: Orchestrate building a new estuary-cdk pull capture connector from a list of streams — research and cluster the streams by shared implementation pattern, scaffold + wire auth, pause for credential entry, then fan out one background stream-builder session per cluster to research/verify/plan, and serially integrate the reviewed plans. Use when creating a brand-new source-* connector that covers several streams.
argument-hint: "[connector-name]  (with the stream list in your prompt)"
allowed-tools: Bash Read Write Edit Glob Grep WebFetch WebSearch Skill Agent AskUserQuestion
---

Build a new pull capture connector `source-$1` covering the streams the user listed. You are the **orchestrator**: you research and cluster the streams, stand up the connector skeleton and auth, then dispatch one background **`stream-builder`** session per cluster to produce reviewed implementation plans, and integrate those plans serially. You hold dispatch authority and own all connector-code edits; the stream-builders only plan.

## Operating principles

- **Stay lean — delegate.** Push verbose work (scaffolding, per-cluster research/verification) to subagents and keep your own context a thin coordination log. Consume their structured summaries, not their transcripts.
- **One hard barrier.** Scaffold + auth must finish, **and the user must have entered real encrypted credentials**, before any `stream-builder` runs (they verify against the live API).
- **Parallel plans, serial integration.** Clusters research and plan concurrently; you integrate one at a time into the shared files (`models.py`/`resources.py`/`test.flow.yaml`), so there are no concurrent-write collisions.
- **You dispatch; stream-builders escalate.** If a cluster recommends a split, you decide whether to dispatch another session.
- **The connector's `bruno/` collection is the home for API knowledge — go there first.** Verified facts about the provider's API (base URL resolution, rate limits, throttling, and per-endpoint limitations) live in `source-$1/bruno/`, next to the requests that prove them: account-wide facts in `bruno/collection.bru`'s `docs` block under `## API constraints (account-wide)`, and endpoint-specific constraints on each proving request under a `**Limitation:**` marker (`grep -rl Limitation bruno/`).
- This skill covers **pull** streams. Webhook-receiver streams are routed to `create-webhook-connector` (see Phase 7).

## Phase 0 — Intake

- Read the connector name (`source-$1`) and the **stream list** from the user's prompt. If the list is missing or vague, ask for it before continuing.
- Identify the provider and confirm `source-$1` doesn't already exist (`ls source-$1`). If it exists, stop — this is a creation skill.
- Open a TODO list: one item per phase, plus one per stream cluster once clustering is settled.

## Phase 1 — Research & Cluster

Research the provider broadly: its **auth** options, **base URL**, **rate limits**, and each stream's backing endpoint (pagination, filters, cursor candidates, response shape). Fan out parallel readers (e.g. `Agent` subagents, one per stream or doc area) if the surface is large — but the clustering judgment is yours.

Then **cluster the streams by shared implementation pattern.** Streams belong in the same cluster when they'd share one polymorphic implementation: same replication strategy (incremental+backfill / incremental-only / snapshot / webhook), same pagination mechanism, same endpoint family, and a compatible cursor model. Auth is connector-wide, so it doesn't partition clusters.

## Phase 2 — Scaffold (barrier, delegated)

Dispatch `scaffold-connector` as a subagent so the boilerplate noise stays out of your context:

> Use the `Agent` tool: _"Invoke the `scaffold-connector` skill for `source-$1`. Set the API base URL to `<url>` if known. Return your structured summary."_

Consume the summary: package name (`$PKG`), the AUTH SEAM locations, and whether `spec`/`discover` passed. Don't proceed if the skeleton doesn't pass its smoke test.

## Phase 3 — Auth

Run the `configure-auth` skill for `source-$1` **yourself** (not as a detached subagent) — it has a user checkpoint (scheme selection) and the credential-entry handoff that you must own. Drive its Phase 2 decision with the user, let it wire `models.py`/`resources.py`/`spec()`, and capture its output (chosen scheme, probe endpoint, any managed-OAuth dependency).

## Phase 4 — Credential pause (mandatory STOP)

`configure-auth` ends here for a reason. **Stop and have the user populate `source-$1/config.yaml` with real credentials and sops-encrypt it** (match a sibling connector's KMS setup). Do **not** dispatch any `stream-builder` until the user confirms the encrypted config is in place — the stream-builders verify against the live API and will otherwise stall or produce `UNVERIFIED` plans. This is the credential half of the barrier.

## Phase 5 — Fan out stream-builders (one background session per cluster)

For each confirmed cluster, dispatch a background session whose main agent is `stream-builder`. Use the dispatch pattern validated for this setup — **no `--add-dir`** (it triggers a folder-trust dialog that stalls the session):

```bash
claude --bg --name "source-$1-<cluster>" --agent stream-builder "<brief>"
```

The `<brief>` must give `stream-builder` everything its "What you receive" section expects:

- connector name `source-$1` and package `$PKG`;
- provider and API base URL;
- the **auth scheme** `configure-auth` wired (so it doesn't re-derive it);
- the **rate-limit budget**;
- the cluster's **stream list** and **why they were grouped** (the shared-pattern hypothesis).

Capture each session's short id from the dispatch output. Tell the user to open `claude agents`, watch the rows, and **answer each session's plan-feedback gate in the peek panel**.

**Monitor** each session by reading `~/.claude/jobs/<short-id>/state.json` (richer than `claude agents --json`):

- `tempo: blocked` → the session is waiting on the user in the peek panel (its plan-feedback gate, or a seeding hand-off). The user sees these directly in `claude agents` — do NOT relay them into the orchestrator chat.
- `state: done` → its plan is ready in `~/.claude/jobs/<short-id>/tmp/`. Verify it actually finalized (a `done` state can also be a crash or an idle between-turns pause — check the plan's STATUS marker and the session's last message).
- `state: failed` → inspect `claude logs <short-id>` and decide whether to re-dispatch.

Poll periodically rather than spinning. **Keep orchestrator-chat updates to a minimum**: the user follows the sessions in the `claude agents` window, not this chat. Speak up only when _orchestrator_ action occurred or is needed — a crash/re-dispatch, a collected plan, an integration step, or a blocker the peek panel doesn't show (e.g. an infrastructure outage). Routine blocked/active flaps, per-command approvals, and gate questions the user can read themselves get no commentary.

**Handle splits:** if a finished session's result carries a `SPLIT RECOMMENDATION`, dispatch a fresh `stream-builder` for the split-off streams (you hold dispatch authority — the session never spawns its own).

## Phase 6 — Collect plans

For each `done` session, read its finalized artifacts from `~/.claude/jobs/<short-id>/tmp/`: `plan.md` (or `plan-<subgroup>.md`), `bruno-manifest.md`, and the `bruno/` copy. A plan marked **UNVERIFIED** means credentials weren't in place when it ran — that shouldn't happen after Phase 4; if it did, get the user to fix credentials and re-dispatch rather than integrating an unverified plan.

## Phase 7 — Serial integration (you, one cluster at a time)

The stream-builders already did `add-stream`'s research, classification, and live verification (its early phases) and handed you a plan. Integrate each plan **yourself, serially**:

- For each stream in the cluster, run the `add-stream` skill **guided by the plan** — skip its re-research (Phases 1–3 are done) and implement from the plan's design: model, fetch function, registration, citing the plan's reference `file:line`. Implement the cluster polymorphically as the plan specifies (one parametrized fetch/model, not N copies).
- Use the `child-entities` skill where a plan flags a parent/child relationship.
- **Webhook-classified streams** don't go through `add-stream` — set them up via `create-webhook-connector`'s webhook machinery instead.
- Merge each session's `bruno/` collection into `source-$1/bruno/` so it's committed with the connector.
- After all clusters are integrated, dispatch the `regenerate-flow-discovery` agent (runs on Haiku in its own context) once to land a consistent generated-schema + snapshot state across the full stream set.

## Phase 8 — Finalize

- Run the full test suite (`poetry run pytest`, output to a file and read it — don't `tail`).
- Confirm the discover snapshot lists every stream; note any quiet streams legitimately absent from the capture snapshot.
- Audit `git diff --stat`: scope it to the new connector, and recommend splitting any unrelated CDK schema sweep into its own commit.
- Hand back a summary: streams delivered (and their classifications), any that split, anything still `UNVERIFIED` or pending seeding, the managed-OAuth dependency if any, and the suggested commit breakdown.

## Out of scope

- Webhook-receiver connector scaffolding → `create-webhook-connector` (which shares `scaffold-connector` + `configure-auth`).
- Provisioning or encrypting the user's real credentials → the user, at the Phase 4 pause.
- Spawning stream-builders that spawn more stream-builders → splits escalate to you; you dispatch.
