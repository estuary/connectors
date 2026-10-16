---
name: create-capture-connector
description: Build a new multi-stream estuary-cdk pull capture connector end to end, from a stream list to a release-ready branch. Use for a brand-new `source-*`; `add-stream` extends an existing one.
argument-hint: "[connector-name] [--autonomous|--human-in-the-loop] [stream-name...]"
allowed-tools: Bash Read Write Edit Glob Grep WebFetch WebSearch Skill Agent AskUserQuestion
---

Build `source-$1` covering the streams the user listed. You are the **orchestrator**: you cluster the streams, stand up the skeleton and auth, dispatch one background **`stream-builder`** session per cluster to plan, and integrate the plans serially. You hold dispatch authority and own every connector-code edit; stream-builders only plan.

## Operating principles

- **Delegate the verbose work.** Scaffolding and per-cluster research run in subagents; you consume their structured summaries and keep your own context a coordination log.
- **Every user ask is a `GATE-*`** in [`interaction-mode.md`](../../shared/interaction-mode.md). Resolve the mode in Phase 0 and forward it in every dispatch (`CONDUCT-FORWARD-MODE`). Autonomous mode interrupts the user once, at Phase 4; every other gate resolves per the table and lands in the ledger.
- **One barrier.** Skeleton, auth wiring and the user's encrypted `config.yaml` are committed before any stream-builder runs.
- **Parallel plans, serial integration.** Clusters plan concurrently in their own worktrees; you integrate one cluster at a time into the shared files, so nothing writes concurrently.
- **Stream-builders escalate; you dispatch.** A `SPLIT RECOMMENDATION` is a request; you decide whether to dispatch for it.
- Pull streams only. Webhook streams route to `create-webhook-connector` (Phase 7).

## Phase 0 — Intake

- **Resolve the interaction mode** per `interaction-mode.md` § Resolving the mode: a flag in the prompt answers the mode question; the questionnaire always asks the seeding question. Record both: `python3 .claude/scripts/permissions.py grant source-$1 --mode <m> --seeding <s>` (`CONDUCT-PERMISSIONS-FILE`).
- Read the connector name and the **stream list**: the remaining positional arguments, or names in the prompt's prose. Missing or vague → `GATE-STREAM-LIST`.
- Confirm `source-$1` does not exist (`ls source-$1`). If it does, stop: this is a creation skill.
- Open the TODO list (`CONDUCT-TODO-LIST`): one item per phase, plus one per cluster once Phase 1 settles them.

## Phase 1 — Research & cluster

Research the provider: auth options, base URL, rate limits, and each stream's backing endpoint (pagination, filters, cursor candidates, response shape). Fan out `Agent` readers when the surface is large; the clustering judgment stays yours.

**Read beyond the official docs.** Support tickets, forums, GitHub issues and other integrations' docs reveal where the API bites: slow endpoints, eventual-consistency lag, undocumented limits, pagination quirks. Before relying on such a report, pin the API version it refers to and check it is still current against the release notes. Confirmed, current pain points go into the affected cluster's brief as risks with mitigations.

**Cluster by shared implementation pattern.** Streams share a cluster when one polymorphic implementation would serve them: same replication strategy (incremental+backfill / incremental-only / snapshot / webhook), same pagination mechanism, same endpoint family, compatible cursor model. Auth is connector-wide and never partitions.

Done when every listed stream has a backing endpoint, its pagination mechanism, a cursor candidate (or "none") and exactly one cluster recorded, and the provider has an auth scheme, base URL and rate-limit budget recorded. A blank entry is research still to do.

## Phase 2 — Scaffold (delegated)

Dispatch `scaffold-connector` as an `Agent` subagent: _"Invoke the `scaffold-connector` skill for `source-$1`. API base URL: `<url>`. Return your structured summary."_ Consume the summary: `$PKG`, the AUTH SEAM locations, and `spec`/`discover` PASS. A failed smoke test is fixed before Phase 3.

## Phase 3 — Auth

Run `configure-auth` for `source-$1` inline, in your own context: it holds `GATE-AUTH-SCHEME`, which you own. Capture its output: scheme, probe endpoint, managed-OAuth dependency, and the credential fields the user must supply.

## Phase 4 — Credentials (`GATE-CREDENTIALS`)

Resolve per the gate table; in autonomous mode this is the batched checkpoint, carrying every decision so far (stream list, clusters, auth scheme, managed-OAuth dependency, `GATE-TIGHT-BUDGET` plan, seeding answer). Orchestrator mechanics around it:

- **Commit before fan-out.** The worktrees Phase 5 creates branch from HEAD, and `stamp` refuses an uncommitted `config.yaml`. Ask the user to commit the skeleton and auth wiring together with the encrypted config, or to confirm you may (`CONDUCT-CONFIRM-BEFORE-WRITE-HISTORY`).
- **Stamp**: `python3 .claude/scripts/permissions.py stamp source-$1` once they confirm. A later re-commit of `config.yaml` means re-confirm and stamp again (`GATE-CONFIG-DIRTY`).
- **"No credentials"** starts a docs-only run: every brief says so, plans arrive PENDING, and Phase 8 takes its docs-only branch.

## Phase 5 — Fan out stream-builders

One background session per cluster; the command, brief contents, monitoring cadence and chat policy are in [`dispatch.md`](dispatch.md). Handle a `SPLIT RECOMMENDATION` by dispatching a fresh stream-builder for the split-off streams.

Phase 5 ends when every session is `done` with a finalized plan, or `failed` and re-dispatched or ledgered.

## Phase 6 — Collect plans

For each finished session read `~/.claude/jobs/<short-id>/tmp/`: `plan.md` (or `plan-<subcluster>.md`), `bruno-manifest.md`, and the `bruno/` copy.

A **PENDING** plan is expected in a docs-only run: integrate it and carry its findings into the ledger. After a real credential checkpoint it means the session could not reach the API; fix the cause and re-dispatch rather than integrate an unverified plan.

**Autonomous mode**: this is the orchestrator's half of `GATE-PLAN-REVIEW`. For each plan dispatch an `Agent` that reads `.claude/shared/rules-index.md` and the plan, checks every `FETCH-*` / `DOC-*` row it can from the text, and returns findings. Fix them in the plan, or re-dispatch the stream-builder for anything needing live evidence. Fold each plan's `## Decisions made without review` into your ledger.

## Phase 7 — Serial integration

The plan replaces `add-stream`'s Phases 0–3 (the baseline suite has nothing to baseline yet, and `GATE-STREAM-DESIGN` was settled at `GATE-PLAN-REVIEW`), and this skill owns its Phases 6–7. One cluster at a time:

- For each stream, run `add-stream` **Phases 4, 5 and 5.5 only**, implementing from the plan's design and citing its reference `file:line`. Implement the cluster polymorphically as the plan specifies: one parametrized fetch and model, not N copies.
- `child-entities` where the plan flags a parent/child relationship.
- Webhook-classified streams bypass `add-stream`; wire them per `create-webhook-connector` Phase 3.
- Merge the session's `bruno/` copy into `source-$1/bruno/`.

Done when every stream in every plan is registered, `basedpyright` is clean per Phase 5.5, and `poetry run flowctl raw spec` passes. Then dispatch the `regenerate-flow-discovery` agent once, with the Phase 1 budget verdict, to land generated schemas and snapshots for the full stream set. In a docs-only run tell it there are no credentials: the scaffold's opt-in `test_capture` stays absent, so it refreshes the spec and discover snapshots only.

## Phase 8 — Finalize

- Run the suite (`poetry run pytest`, output to a file, read the file). Docs-only run: spec and discover snapshots are the suite; the capture test is added when credentials arrive.
- **Lint:** once the suite passes, run `pipx run ruff==0.16.9 check source-$1/` from the repo root (output to a file and read it). Fix findings in code this session wrote; report pre-existing ones to the user without fixing them.
- Confirm the discover snapshot lists every stream; note quiet streams legitimately absent from the capture snapshot.
- Release requirements per [`release.md`](release.md): CI registration, `CHANGELOG.md`, docs page.
- `git diff --stat` scoped to the new connector; an unrelated CDK schema sweep is its own commit (`GATE-COMMIT-SPLIT`).
- **Autonomous mode**: dispatch `pr-review-toolkit:code-reviewer` over the full diff, telling it to read `.claude/shared/rules-index.md` first, and fix what it finds. This stands in for the eyes a human-in-the-loop user had on each plan.
- Apply the hand-off rule in [`evidence-markers.md`](../../shared/evidence-markers.md): no PENDING finding remains except the docs-only and unrun-seeding cases, each listed in the summary.
- `python3 .claude/scripts/permissions.py revoke source-$1`.
- Hand back: streams delivered with classifications, splits, remaining PENDING findings, the managed-OAuth dependency, and the suggested commit breakdown. Autonomous mode ends with `## Decision ledger` (`interaction-mode.md` § Decision ledger).

## Out of scope

- Webhook-receiver connector scaffolding → `create-webhook-connector`.
- Provisioning or encrypting real credentials → the user, at Phase 4.
- Stream-builders dispatching stream-builders → splits escalate to you.
