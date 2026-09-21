# Session conduct

How to run a connector session. Rules tagged `conduct-only` leave no trace in a diff — they are the `conduct` checkability class in [rules-index.md](rules-index.md#checkability); a reviewer must not comment on them.

---

### `CONDUCT-TODO-LIST` · conduct-only

Open the session with a TODO list: one task per phase of whatever skill you're running, plus any provider-specific work that comes up.

---

### `CONDUCT-CONSENT-DESTRUCTIVE` · conduct-only

Ask the user for consent before any destructive operation — deleting generated files or directories, rewriting bindings, mutating provider state.

Provider-state mutation additionally falls under `API-NEVER-MUTATE`.

---

### `CONDUCT-CONFIRM-BEFORE-WRITE-HISTORY` · conduct-only

Never push or open a PR without explicit user confirmation given in the current session — no exception, regardless of how routine the change is.

Committing locally needs the same confirmation, **except** a commit that is purely mechanical and touches nothing else: a formatter/linter pass or a snapshot/schema regeneration with no other change mixed in. Those may be committed on their own without asking.

Anything else — a code change, a doc change, a rule change, or a commit that mixes substance with the mechanical output above — still needs confirmation first. Make the edits, leave them unstaged, and report which files/hunks are yours. When a diff is a mix and you're unsure whether it counts as "purely mechanical," ask.

---

### `CONDUCT-STATE-CONVENTIONS` · conduct-only

State out loud when you're deliberately following an existing convention. State equally out loud when you're deviating, and justify it. Silence is worse than confirmation — a reviewer cannot tell a considered deviation from an accident.

---

### `CONDUCT-CITE-SOURCE` · checkable-from-diff

When you copy a pattern from another stream or connector, cite the exact `file:line` you copied from so the user can sanity-check the transfer.

Checkable from a diff in one direction only: a new fetch function that structurally mirrors a sibling but cites nothing is a finding. The reviewer cannot verify a citation points at the _right_ line without reading it, so it should follow the citation and confirm rather than assume.

---

### `CONDUCT-GREP-BEFORE-CITE` · conduct-only

If you propose a function, field, or flag that you _remember_ seeing in the connector, grep for it before recommending it. The codebase may have changed since that memory formed, and a confidently-cited symbol that no longer exists costs more trust than an admitted uncertainty.

---

### `CONDUCT-ESTABLISH-BASELINE` · conduct-only

Kick off the connector's existing test suite (typically `pytest` from the connector directory) in the background as soon as the session's first phase begins. Read-only and planning phases may proceed in parallel while it runs — do **not** block on it. The result is a **gate on the first code edit**: confirm the baseline passes before writing anything, so later failures are attributable to your change, not pre-existing drift.

If the suite fails:

- **Simple schema drift** (spec/discover snapshots have shifted but the code is unchanged): dispatch the `regenerate-flow-discovery` agent to refresh them, then commit that alone — message `<connector-name>: update tests` — **before any other work**, so the real change diffs against a green baseline and stays scoped (`REVIEW-SNAPSHOT-SCOPE`).
- **Flakey fields in the diff** (timestamps like `updated_at`, ETags, anything that changes between runs): surface them and ask whether to add them to the connector's `FIELDS_TO_REDACT` list in its snapshot test module (name varies — grep for `FIELDS_TO_REDACT`).
- **Anything else:** stop and surface the failure before editing; a pre-existing failure changes what "done" or "fixed" means.

One more check before the first edit: **format the files you're about to touch**. For Python connectors we use `ruff format`. If that produces a diff, commit it alone — message `<connector-name>: formatting` — before your edits, so the real change diffs clean instead of mixing fix and cleanup.

---

### `CONDUCT-VERIFY-FIRST` · conduct-only

Verify first, code second. For anything the provider's API does, the Bruno collection is the source of truth for "what does this endpoint actually return today" — not the docs, and not recall. See [provider-api-consent.md](provider-api-consent.md) for the rules on getting that evidence.
