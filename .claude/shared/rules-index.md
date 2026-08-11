# Connector rule index

Every house rule with a stable ID, what evidence it needs, and where its authoritative text lives. **This file relocates nothing** — it's a lookup so a reviewer can enumerate the rule set without invoking six skills, and so a citation from a months-old review comment still resolves.

`grep -rn '<RULE-ID>' .claude/` finds any rule's home.

## How to use this as a reviewer

`paths`-scoped rules only load when a matching file passes through Read / Edit / Write — **not** on Bash access. A review that starts with `git diff` therefore has no ambient rules. Read this index explicitly as step one, then open only the sections the diff actually touches.

## Checkability

- **`diff`** — the diff alone is sufficient evidence. Assert violations freely.
- **`runtime`** — needs a Bruno probe, live response, or cited provider doc. Flag *missing evidence*; never assert a violation without it.
- **`conduct`** — about how the session was run. Invisible in a diff. **Do not comment on these during review.**

A review must state which rules it could not check, rather than silently limiting itself to the `diff` class.

## Index

### Always-on — [`.claude/rules/connector-python.md`](../rules/connector-python.md)

Auto-loads on `source-*/**/*.py`. Short rules where silence produces a bug.

| Rule | Check | Statement |
| ---- | ----- | --------- |
| no-json-loads | diff | Parse responses with a Pydantic model, never `json.loads` |
| required-not-getattr | diff | Fields the logic depends on are required, not `getattr(o, "f", None)` |
| incremental-processors | diff | Prefer `IncrementalJsonProcessor` / `IncrementalCsvProcessor` |
| basedocument-extra-allow | diff | Documents subclass `BaseDocument`, `extra="allow"`, minimal fields |
| snake-case-log-keys | diff | Structured-log keys are snake_case |
| pep695-generics | diff | `class Foo[T]`, never `TypeVar` + `Generic[...]` |
| fetch-naming | diff | `fetch_<s>` / `backfill_<s>` / `snapshot_<s>` |
| narrow-api-params | diff | `api.py` takes only the config fields it reads |
| param-order | diff | `log`, `http`, `config` — CDK fetch contracts override |
| propagate-errors | diff | `ValidationError` for config; let `HTTPError` propagate; never swallow |

### Fetch layer — [`fetch-function-rules` skill](../skills/fetch-function-rules/SKILL.md)

Invoke before writing or changing any `fetch_*` / `backfill_*` function.

| Rule | Check | Statement |
| ---- | ----- | --------- |
| `FETCH-MODEL-OWNS-CONTRACT` | diff | Paths, items keys, cursor params are ClassVars; cursor via `get_cursor()` |
| `FETCH-PARAM-DICT-NAMING` | diff | `params` at the call site; `request_params` when it crosses a boundary |
| `FETCH-COMPLETE-TICKS` | diff | Cap the window at the last fully-elapsed tick; early-return if `horizon <= cursor` |
| `FETCH-SERVER-SIDE-BOUNDARIES` | runtime | Boundaries in the request, not client-side filters; probe inclusivity first |
| `FETCH-SEAM-PRECISION` | diff | Gapless at cutoff and cursor; backfill start is not load-bearing |
| `FETCH-BACKFILL-ALONGSIDE` | diff | Implement `fetch_page` alongside `fetch_changes` |
| `FETCH-CURSOR-MUST-BE-UPDATED` | diff | `created_at` is not a usable incremental cursor |
| `FETCH-LOGCURSOR-AFTER-DOCS` | diff | A LogCursor must be yielded after documents are emitted |
| `FETCH-VALUE-WATERMARK-RESUME` | diff | Resume by value watermark, not positional offset — **data-loss-class** |
| `FETCH-CHECKPOINT-STABLE-STATE` | diff | A `PageCursor` must mean the same thing after a resume gap — **data-loss-class** |
| `FETCH-PAGE-SIZE-RESEARCH` | runtime | Max page size and enforcement mode, per endpoint |
| `DOC-FLAG-ONLY-UNVERIFIED` | diff | No "(verified live)" stamps; annotate only what could *not* be verified |
| `DOC-CONTRACT-NOT-MECHANISM` | diff | Docstrings state guarantees, not the current implementation's steps |
| `DOC-RAIL-TIMELINE` | diff | Windows documented as a rail timeline, ≤ 79 cols |

### Provider API — [`provider-api-consent.md`](provider-api-consent.md)

| Rule | Check | Statement |
| ---- | ----- | --------- |
| `API-CONFIG-GATE` | conduct | Read-only calls only while `config.yaml` is clean and tracked |
| `API-NEVER-MUTATE` | conduct | The user runs mutations, not you — consent doesn't lift this |
| `API-ROUTE-THROUGH-BRUNO` | diff | No ad-hoc `curl` against the provider in committed files |
| `API-TOKEN-EPHEMERAL` | diff | Token on `req` only; never `setVar` / `setEnvVar` / committed |
| `API-DONT-READ-CREDS` | conduct | Don't read the encrypted credentials file directly |
| `API-BUDGET-20RPH` | runtime | Rate-limit survey sets the run budget |
| `API-DISCOVER-STATIC-IS-FREE` | diff | `flowctl raw discover` follows the same budget gate as `preview` — free only if this connector's discovery makes zero provider calls |
| `API-COST-SAVER-DISABLE` | diff | `disable: true` is a test-time tool — **never commit it** |

### Session conduct — [`session-conduct.md`](session-conduct.md)

| Rule | Check | Statement |
| ---- | ----- | --------- |
| `CONDUCT-TODO-LIST` | conduct | Open with a TODO list, one task per phase |
| `CONDUCT-CONSENT-DESTRUCTIVE` | conduct | Consent before destructive ops |
| `CONDUCT-CONFIRM-BEFORE-WRITE-HISTORY` | conduct | Push/PR always need confirmation; a purely formatting- or snapshot-only commit doesn't |
| `CONDUCT-STATE-CONVENTIONS` | conduct | Say when you follow a convention, and when you deviate |
| `CONDUCT-CITE-SOURCE` | diff | Copied patterns cite `file:line` |
| `CONDUCT-GREP-BEFORE-CITE` | conduct | Grep for remembered symbols before recommending them |
| `CONDUCT-VERIFY-FIRST` | conduct | Live behavior over docs over recall |

### Situational skills

Not rule lists — invoke when the situation applies, and audit against them when the diff shows the corresponding pattern.

| Situation | Skill |
| --------- | ----- |
| Child stream needing a parent ID to list | [`child-entities`](../skills/child-entities/SKILL.md) |
| Choosing a replication strategy | [`classify-stream-types`](../skills/classify-stream-types/SKILL.md) |
| Wiring credentials / OAuth | [`configure-auth`](../skills/configure-auth/SKILL.md) |
| Verifying an endpoint against live behavior | [`bruno-probe-endpoint`](../skills/bruno-probe-endpoint/SKILL.md) |

### Cross-cutting review sweeps

| Rule | Check | Statement |
| ---- | ----- | --------- |
| `REVIEW-SIBLING-STREAMS` | diff | A fix to one stream's fetch/parse/cursor logic probably applies to its siblings — streams in a connector share polling, pagination, and cursor patterns. Check every sibling and report the ones that look affected. |
| `REVIEW-SNAPSHOT-SCOPE` | diff | `git diff --stat` on regenerated snapshots should be scoped to the change. Uniform per-file deltas across many files are a CDK schema sweep and belong in their own commit. |
| `REVIEW-CHANGELOG-ENTRY` | diff | A user-visible connector change needs a `CHANGELOG.md` entry (`changelog` skill). |
