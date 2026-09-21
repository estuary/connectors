---
name: bugfix-capture-connector
description: Fix a bug in an existing estuary-cdk capture connector — reproduce it, locate it, fix it to house style, sweep sibling streams for the same defect, then regenerate discovery, refresh snapshots, and add a changelog entry. Use when a connector misbehaves in production or a capture produces wrong/missing documents.
argument-hint: "[connector-name] [what's wrong]"
allowed-tools: Bash Read Write Edit Glob Grep Agent Skill WebFetch
---

Fix the reported bug in `source-$1`. This skill is an orchestrator — most steps are existing skills. Don't reimplement them.

## Laws

- [`.claude/shared/session-conduct.md`](../../shared/session-conduct.md) — TODO list, consent, citing sources.
- [`.claude/shared/provider-api-consent.md`](../../shared/provider-api-consent.md) — the `config.yaml` gate, never running mutations, the budget rule.

Plus one specific to fixing rather than building:

**Reproduce before you fix.** A fix for a bug you haven't observed is a guess that also removes the evidence. If you cannot reproduce it, say so explicitly and treat the fix as a hypothesis the user must accept as such.

## Phase 0 — Characterize

Establish what's actually wrong before touching code:

- **Which stream(s)**, and which part — fetch, parse, cursor, wiring, schema?
- **Symptom class**: missing documents, duplicate documents, wrong field values, a crash, a stall, or a capture that never advances its cursor. Missing documents and stalled cursors are the data-loss cases — treat them as urgent and expect the root cause to be in the fetch layer's window or checkpoint logic.
- **Since when** — is this a regression? `git log --oneline -- <connector>/` and check whether a recent change touched the affected stream.

Run the connector's test suite in the background now, as a baseline (`CONDUCT-ESTABLISH-BASELINE`). A pre-existing failure changes what "fixed" means.

## Phase 1 — Reproduce

Prefer the cheapest reproduction that shows the defect:

1. **A test** — if the bug is in parsing, cursor arithmetic, or model validation, it reproduces in `pytest` without touching the provider.
2. **`flowctl raw preview-next`** on the affected binding, per the budget rule. Use `disable: true` on unrelated bindings (`API-COST-SAVER-DISABLE`) to keep it cheap — and restore them before committing.
3. **A Bruno probe** — when the question is what the provider actually returns, use `bruno-probe-endpoint`. Live behavior settles doc-vs-reality disputes.

Record what you observed. That's the before-state the fix has to change.

## Phase 2 — Locate

Read the affected fetch function and its siblings *before* editing. The bug is often in the difference between them.

If the defect is in fetch or cursor logic, **invoke the `fetch-function-rules` skill now** and check the current implementation against it. Bugs of this class are usually an existing rule being violated, not a novel problem — and the rule text tells you what the correct shape is, which saves inventing one.

## Phase 3 — Fix

**Baseline gate (`CONDUCT-ESTABLISH-BASELINE`).** The Phase 0 baseline must have completed and been inspected before the first edit. Pre-existing drift commits first, before any fix work: stale snapshots as `source-$1: update tests`; and run the formatter over the files you're about to edit, committing any resulting diff as `source-$1: formatting`. Full failure handling is in [session-conduct.md](../../shared/session-conduct.md).

Match the surrounding code. `.claude/rules/connector-python.md` auto-loads while you edit and carries the always-apply rules; `fetch-function-rules` carries the window and checkpoint semantics.

Two failure modes specific to bugfixing:

- **Don't broaden the fix into a refactor.** A scoped diff is reviewable and revertable. If the code around the bug is bad, say so and propose it separately.
- **Don't fix the symptom.** A missing document caused by a window that skips a tick is not fixed by re-emitting the last page; it's fixed at the boundary (`FETCH-COMPLETE-TICKS`, `FETCH-SEAM-PRECISION`).

## Phase 4 — Sibling sweep

**The step that makes this skill worth having.** Streams in a connector share polling, pagination, and cursor patterns, so the bug you just fixed is probably present in its siblings.

1. Enumerate the connector's other streams.
2. Check each for the same defect shape — same window arithmetic, same resume key, same parse assumption.
3. Present the affected siblings to the user **before** fixing them: a one-stream bug report becoming a five-stream diff needs consent, and the user may want them split across commits.

Report the sweep result either way. "Checked all six streams, only `campaigns` was affected" is a useful finding.

## Phase 5 — Verify

- The reproduction from Phase 1 now shows the correct behavior.
- The full suite passes, matching the Phase 0 baseline plus the fix.
- If the fix changed discovered schemas or bindings, dispatch the `regenerate-flow-discovery` agent, then confirm `git diff --stat` is scoped to the change (`REVIEW-SNAPSHOT-SCOPE` — a broad uniform delta is a CDK schema sweep and belongs in its own commit).

## Phase 6 — Land

- **Changelog:** invoke the `changelog` skill. Don't pre-judge a fix as too internal to be worth mentioning — if the connector keeps a `CHANGELOG.md`, the fix gets an entry (`REVIEW-CHANGELOG-ENTRY`). Adding the file to a connector that has none stays the user's opt-in call.
- **Self-review:** invoke `review-connector-change` on the working diff before handing off.
- Commit scoped to the fix. Snapshot regeneration unrelated to the bug goes in its own `source-$1: update tests` commit, ordered before the fix (see the Phase 3 baseline gate).
- **Documentation:** if the fix changes user-visible behavior described in the connector's docs, update them (connector docs are mirrored into the flow repo's `site/docs` via a sibling PR).
- **PR body:** follow [.github/pull_request_template.md](../../../.github/pull_request_template.md). Fill every section — drop **Regression?** only when the fix isn't one; list created or affected docs under **Documentation links affected:**; and state under **Notes for reviewers:** how the change was tested (unit tests, snapshot tests, `flowctl raw preview-next`, local stack tests, or another method — name what actually ran).
- **Never push or open a PR without asking.** Always stop and confirm with the user before `git push` or `gh pr create` — even when the fix is verified and the user asked for the fix in the first place.

## Out of scope

- Adding a new stream → `add-stream`.
- A bug in `estuary-cdk` itself rather than a connector — surface it; the blast radius is every connector and that's the user's call.
