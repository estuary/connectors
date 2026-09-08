---
name: create-capture-connector
description: Orchestrate building a new estuary-cdk pull capture connector from a list of streams — research and cluster the streams by shared implementation pattern, scaffold + wire auth, pause for credential entry, then fan out one background stream-builder session per cluster to research/verify/plan, and serially integrate the reviewed plans. Use when creating a brand-new source-* connector that covers several streams.
argument-hint: "[connector-name] [--autonomous|--human-in-the-loop] [stream-name...]"
allowed-tools: Bash Read Write Edit Glob Grep WebFetch WebSearch Skill Agent AskUserQuestion
---

Build a new pull capture connector `source-$1` covering the streams the user listed. You are the **orchestrator**: you research and cluster the streams, stand up the connector skeleton and auth, then dispatch one background **`stream-builder`** session per cluster to produce reviewed implementation plans, and integrate those plans serially. You hold dispatch authority and own all connector-code edits; the stream-builders only plan.

## Operating principles

- **Stay lean — delegate.** Push verbose work (scaffolding, per-cluster research/verification) to subagents and keep your own context a thin coordination log. Consume their structured summaries, not their transcripts.
- **Two users, one contract.** Every point where this skill or a sub-skill would ask the user is a `GATE-*` in [`interaction-mode.md`](../../shared/interaction-mode.md). Resolve the mode once in Phase 0 and forward it in every dispatch (`CONDUCT-FORWARD-MODE`). In autonomous mode the user is interrupted exactly once, at Phase 4; everything else is batched into that checkpoint or decided per the gate table and ledgered.
- **One hard barrier.** Scaffold + auth must finish, **and the user must have entered real encrypted credentials**, before any `stream-builder` runs (they verify against the live API).
- **Parallel plans, serial integration.** Clusters research and plan concurrently; you integrate one at a time into the shared files (`models.py`/`resources.py`/`test.flow.yaml`), so there are no concurrent-write collisions.
- **You dispatch; stream-builders escalate.** If a cluster recommends a split, you decide whether to dispatch another session.
- **`source-$1/bruno/` is the home for API knowledge — go there first.** Rate limits, throttling and per-endpoint limitations live next to the requests that prove them, with the layout and finding taxonomy defined in `bruno-probe-endpoint`.
- This skill covers **pull** streams. Webhook-receiver streams are routed to `create-webhook-connector` (see Phase 7).

## Phase 0 — Intake

- **Resolve the interaction mode** first, per `interaction-mode.md` § Resolving the mode: a flag in the prompt answers the mode question; the questionnaire always asks the seeding question. Record both with `python3 .claude/scripts/permissions.py grant source-$1 --mode <m> --seeding <s>` (`CONDUCT-PERMISSIONS-FILE`).
- Read the connector name (`source-$1`) and the **stream list** — the remaining positional arguments, or names given in the prompt's prose. If the list is missing or vague → `GATE-STREAM-LIST`.
- Identify the provider and confirm `source-$1` doesn't already exist (`ls source-$1`). If it exists, stop — this is a creation skill.
- Open a TODO list: one item per phase, plus one per stream cluster once clustering is settled.

## Phase 1 — Research & Cluster

Research the provider broadly: its **auth** options, **base URL**, **rate limits**, and each stream's backing endpoint (pagination, filters, cursor candidates, response shape). Fan out parallel readers (e.g. `Agent` subagents, one per stream or doc area) if the surface is large — but the clustering judgment is yours.

**Read beyond the official docs for known API pain.** Provider docs describe the happy path; support tickets, community forums, GitHub issues, and other integrations' documentation reveal where the API actually bites — slow endpoints, eventual-consistency lag, undocumented limits, pagination quirks. Search those sources during research to see whether others are struggling with the same endpoints. When you find such a report, two rules before you rely on it: (a) pin the API **version** it refers to and confirm it applies to the one you're building on (an issue on a sunset version may be irrelevant); and (b) check whether it's **still current** — cross-reference the provider's release notes and, where possible, a live probe, since providers do fix things. Fold confirmed, current pain points into the affected cluster's plan as risks with their mitigation.

Then **cluster the streams by shared implementation pattern.** Streams belong in the same cluster when they'd share one polymorphic implementation: same replication strategy (incremental+backfill / incremental-only / snapshot / webhook), same pagination mechanism, same endpoint family, and a compatible cursor model. Auth is connector-wide, so it doesn't partition clusters.

## Phase 2 — Scaffold (barrier, delegated)

Dispatch `scaffold-connector` as a subagent so the boilerplate noise stays out of your context:

> Use the `Agent` tool: _"Invoke the `scaffold-connector` skill for `source-$1`. Set the API base URL to `<url>` if known. Return your structured summary."_

Consume the summary: package name (`$PKG`), the AUTH SEAM locations, and whether `spec`/`discover` passed. Don't proceed if the skeleton doesn't pass its smoke test.

## Phase 3 — Auth

Run the `configure-auth` skill for `source-$1` **yourself** (not as a detached subagent) — it has a user checkpoint (scheme selection, `GATE-AUTH-SCHEME`) and the credential-entry handoff that you must own. Resolve its Phase 2 decision per `GATE-AUTH-SCHEME`, let it wire `models.py`/`resources.py`/`spec()`, and capture its output (chosen scheme, probe endpoint, any managed-OAuth dependency).

## Phase 4 — Credential pause (mandatory STOP)

`configure-auth` ends here for a reason. **Stop and have the user populate `source-$1/config.yaml` with real credentials and sops-encrypt it** (match a sibling connector's KMS setup). Do **not** dispatch any `stream-builder` until the user confirms the encrypted config is in place — the stream-builders verify against the live API and will otherwise stall or produce `PENDING` plans. This is the credential half of the barrier.

This is `GATE-CREDENTIALS`. In **autonomous mode it is the run's single interruption**: compose it per `interaction-mode.md` § The batched checkpoint, carrying every decision deferred so far (stream list, clusters, auth scheme, managed-OAuth dependency, `GATE-TIGHT-BUDGET` plan, the seeding answer). When the user confirms, run `python3 .claude/scripts/permissions.py stamp source-$1` — it refuses until `config.yaml` is committed, and binds the seeding permission to exactly those credentials; if they later re-commit the file, ask them to re-confirm and stamp again. If the user declines credentials ("no credentials"), that's a legitimate docs-only run: note it in each brief, expect PENDING plans, and open the final ledger with it.

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
- the cluster's **stream list** and **why they were grouped** (the shared-pattern hypothesis);
- the **interaction mode** and **seeding answer** (`interaction mode: human-in-the-loop|autonomous`, `seeding: assistant|user|none` — `CONDUCT-FORWARD-MODE`).

Capture each session's short id from the dispatch output. In human-in-the-loop mode, tell the user to open `claude agents`, watch the rows, and **answer each session's plan-feedback gate in the peek panel**. In autonomous mode there is nothing for them to answer — the sessions self-review and finish; say nothing.

**Monitor** each session by reading `~/.claude/jobs/<short-id>/state.json` (richer than `claude agents --json`):

- `tempo: blocked` → the session is waiting on the user in the peek panel (its plan-feedback gate, or a seeding hand-off). The user sees these directly in `claude agents` — do NOT relay them into the orchestrator chat.
- `state: done` → its plan is ready in `~/.claude/jobs/<short-id>/tmp/`. Verify it actually finalized (a `done` state can also be a crash or an idle between-turns pause — check the plan's STATUS marker and the session's last message).
- `state: failed` → inspect `claude logs <short-id>` and decide whether to re-dispatch.

Poll periodically rather than spinning. **Keep orchestrator-chat updates to a minimum**: the user follows the sessions in the `claude agents` window, not this chat. Speak up only when _orchestrator_ action occurred or is needed — a crash/re-dispatch, a collected plan, an integration step, or a blocker the peek panel doesn't show (e.g. an infrastructure outage). Routine blocked/active flaps, per-command approvals, and gate questions the user can read themselves get no commentary.

**Handle splits:** if a finished session's result carries a `SPLIT RECOMMENDATION`, dispatch a fresh `stream-builder` for the split-off streams (you hold dispatch authority — the session never spawns its own).

## Phase 6 — Collect plans

For each `done` session, read its finalized artifacts from `~/.claude/jobs/<short-id>/tmp/`: `plan.md` (or `plan-<subgroup>.md`), `bruno-manifest.md`, and the `bruno/` copy. A plan marked **PENDING** means credentials weren't in place when it ran. In a docs-only run (the user declined credentials at Phase 4) that's expected — integrate it and carry the PENDING findings into the ledger. Otherwise it shouldn't happen after Phase 4; get the user to fix credentials and re-dispatch rather than integrating an unverified plan.

**Autonomous mode — review before integrating (`GATE-PLAN-REVIEW`).** No human read these plans, so you supply the review: for each plan, dispatch an `Agent` subagent that reads `.claude/shared/rules-index.md` and the plan, checks every `FETCH-*`/`DOC-*` row it can from the plan's text, and returns findings. Fix findings yourself in the plan (or re-dispatch the stream-builder for anything needing live evidence). Fold each plan's `## Decisions made without review` into your decision ledger.

## Phase 7 — Serial integration (you, one cluster at a time)

The stream-builders already did `add-stream`'s research, classification, and live verification (its early phases) and handed you a plan. Integrate each plan **yourself, serially**:

- For each stream in the cluster, run the `add-stream` skill **guided by the plan** — skip its re-research (Phases 1–3 are done) and implement from the plan's design: model, fetch function, registration, citing the plan's reference `file:line`. Implement the cluster polymorphically as the plan specifies (one parametrized fetch/model, not N copies).
- Use the `child-entities` skill where a plan flags a parent/child relationship.
- **Webhook-classified streams** don't go through `add-stream` — set them up via `create-webhook-connector`'s webhook machinery instead.
- Merge each session's `bruno/` collection into `source-$1/bruno/` so it's committed with the connector.
- After all clusters are integrated, dispatch the `regenerate-flow-discovery` agent (runs on Haiku in its own context) once to land a consistent generated-schema + snapshot state across the full stream set.

## Phase 8 — Finalize

- Run the full test suite (`poetry run pytest`, output to a file and read it — don't `tail`).
- **Lint:** once the suite passes, run `pipx run ruff==0.16.9 check source-$1/` from the repo root (output to a file and read it). Fix findings in code this session wrote; report pre-existing ones to the user without fixing them.
- Confirm the discover snapshot lists every stream; note any quiet streams legitimately absent from the capture snapshot.
- **Release requirements** — every new connector ships with all three; `source-zuora`'s introduction is the reference shape for each:
  - **CI registration**: add an entry to `.github/python-connectors.yaml`, the list `.github/workflows/python.yaml` builds its matrix from (`name`, `type: capture`, `version` matching the connector's `VERSION` file, `usage_rate: "1.0"`).
  - **CHANGELOG.md**: create `source-$1/CHANGELOG.md` (`# Changelog`, `## <today>`, `### Added` — "Initial release of the <Provider> capture connector."). Convention: [CONTRIBUTING.md](../../../CONTRIBUTING.md#changelog-entries). Creating the file is this skill's job: the `changelog` skill deliberately refuses to add one to a connector that lacks it, treating that as the new-connector opt-in that happens here.
  - **Docs page**: write `docs/reference/Connectors/capture-connectors/<provider>.md` (append `-native` when a legacy connector already owns the plain name), using `iterable-native.md` as the boilerplate reference — adapt its text only where the provider's facts differ: `description:` frontmatter, supported-resources table with replication modes (link each resource to its API reference page — take the URLs from the citations in `source-$1/bruno/`, don't guess them), a `:::tip` for scheduled-backfill/cursor caveats, prerequisites, endpoint + bindings property tables (include the `credentials_title` discriminator row when auth is a union), and a sample capture spec. Docs are additionally mirrored into the flow repo (`site/docs`) via a sibling PR at publish time.
- Audit `git diff --stat`: scope it to the new connector; an unrelated CDK schema sweep goes in its own commit (`GATE-COMMIT-SPLIT`).
- **Autonomous mode:** run a review pass over the full diff before hand-off — dispatch `pr-review-toolkit:code-reviewer` (or an `Agent` reading `rules-index.md`) and fix what it finds. This replaces the eyes a human-in-the-loop user would have had on each plan.
- Confirm no `**PENDING:**` findings remain in `source-$1/bruno/` (`grep -rl '\*\*PENDING' source-$1/bruno/ | grep -v opencollection.yml` must be empty) — resolve or reclassify to UNOBSERVABLE before hand-off. Exception: a docs-only run keeps its PENDING findings (they're honest — the evidence was never collected) and lists every one in the hand-off summary.
- `python3 .claude/scripts/permissions.py revoke source-$1` — the permission was for this run only.
- Hand back a summary: streams delivered (and their classifications), any that split, anything still `PENDING` or awaiting seeding, the managed-OAuth dependency if any, and the suggested commit breakdown. In autonomous mode, end with the `## Decision ledger` (`interaction-mode.md` § Decision ledger) — it's the only place the user learns which streams became snapshots, which findings stayed unverified, and why.

## Out of scope

- Webhook-receiver connector scaffolding → `create-webhook-connector` (which shares `scaffold-connector` + `configure-auth`).
- Provisioning or encrypting the user's real credentials → the user, at the Phase 4 pause.
- Spawning stream-builders that spawn more stream-builders → splits escalate to you; you dispatch.
