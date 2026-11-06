---
name: fetch-function-rules
description: The house rules for estuary-cdk incremental and backfill fetch functions — window semantics (complete ticks, server-side boundaries, seam precision), backfill resume keys, checkpoint stability, and the rail-timeline docstring format. Invoke before writing or changing any `fetch_*` / `backfill_*` function; several of these rules are data-loss-class and none are inferable from surrounding code. Also the reference a reviewer audits fetch-layer changes against.
allowed-tools: Read Grep Glob
---

# Fetch function house rules

Rules for `fetch_*` (incremental) and `backfill_*` (backfill) functions in estuary-cdk connectors. Each carries a stable ID — **cite the ID, never a position** — and a checkability tag telling a reviewer what evidence it needs:

- `checkable-from-diff` — the diff alone is sufficient; a reviewer may assert a violation.
- `needs-runtime-evidence` — requires a Bruno probe, live response, or cited doc. A reviewer may flag missing evidence but must not assert a violation without it.

---

## Shape

### `FETCH-MODEL-OWNS-CONTRACT` · checkable-from-diff

**The model owns its endpoint contract.** Path templates, items keys, and cursor-filter parameter names (`SINCE_PARAM` / `BEFORE_PARAM`-style) are ClassVars on the document model; the cursor value comes from an abstract `get_cursor()` method on the model base — never a `cursor_of` callable threaded through signatures.

### `FETCH-PARAM-DICT-NAMING` · checkable-from-diff

**Name request-param dicts by distance from the request.** Plain `params` only where the dict is handed directly to the HTTP call; `request_params` / `extra_request_params` when it crosses a signature, struct, or partial. No anonymous tuples in wiring maps — use a small frozen dataclass.

---

## Window semantics (updated-style cursors)

"Tick" means one unit of the provider's cursor resolution — **determine it first**; providers variously use seconds, milliseconds, or whole minutes.

### `FETCH-COMPLETE-TICKS` · checkable-from-diff

**Fetch complete ticks only.** Cap every incremental window at the last fully-elapsed tick (at second resolution, `horizon = now() floored to the second − 1s`); early-return when `horizon <= cursor`.

Updates always stamp their own "now", so an elapsed tick is final the moment it is read — no boundary re-fetch or re-emission tricks are ever needed.

### `FETCH-SERVER-SIDE-BOUNDARIES` · needs-runtime-evidence

**Boundaries live in the request, not in client-side filters.** First pin each boundary param's inclusivity with a cheap probe (query with the bound set to a document's exact cursor timestamp; check membership), then choose the queried timestamps so the server returns exactly the intended window.

Client filtering is a second boundary-logic layer plus wasted bandwidth and memory — it is only the fallback while semantics are still unverified.

Runtime evidence required: the probe that established each boundary's inclusivity. Without it a reviewer can only flag that the evidence is missing.

### `FETCH-SEAM-PRECISION` · checkable-from-diff

**Exact precision at the seams only.** The seams — backfill→incremental at the cutoff, poll→poll at the cursor — must be gapless: floor the cutoff to a whole tick, end backfill at `cutoff − 1 tick` (inclusive), seed the first incremental cursor at `cutoff − 1 tick` so its first emitted tick is exactly `cutoff`.

The **start** of a backfill window is not load-bearing: query `since = start_date` as-is and let the boundary instant fall out. Don't add `− 1 tick` machinery for it.

### `FETCH-BACKFILL-ALONGSIDE` · checkable-from-diff

When implementing an incremental stream, always try to implement a backfill function (`fetch_page`) alongside `fetch_changes`. The backfill collects historical data up to the log cursor.

### `FETCH-CURSOR-MUST-BE-UPDATED` · checkable-from-diff

Cursors are normally `updated_at`-style. **A `created_at` timestamp is useless for incremental sync** — you'd have to traverse the entire dataset to catch updates to older records. Where only `created_at` exists, use a snapshot stream instead (which also captures deletions), or pair incremental with a scheduled backfill.

### `FETCH-LOGCURSOR-AFTER-DOCS` · checkable-from-diff

`fetch_changes` may return without yielding a LogCursor if no documents were emitted. A LogCursor **must** be yielded after documents are emitted.

---

## Resumption and checkpointing

### `FETCH-VALUE-WATERMARK-RESUME` · checkable-from-diff

**Resume a backfill by a value-based key watermark (`key > watermark`), not a positional offset.**

The positional `count=M&offset=N` resume is the real hazard: a row deleted mid-backfill renumbers everything after it, so on resume you skip an _innocent bystander_ row that was never touched — and because that row wasn't mutated, the incremental task won't catch it either. It's genuinely lost. A value-watermark resume (`create_time` / `send_time` / `updated_at` `> watermark`, with an id tie-break) is deletion-proof: a deletion drops an entry but renumbers nothing.

Whether the ordering key is immutable or mutable is a _secondary_ choice — both terminate over a frozen `[start_date, cutoff]` window. An immutable key (`create_time`, `send_time`) processes every windowed item exactly once, so the backfill is self-contained. A mutable key (`updated_at`) lets items updated mid-sweep leave the window, so the range query drains to empty — the backfill still terminates, it just hands those churned items to the incremental task. Pick immutable when you want the backfill self-contained; mutable is fine wherever update-induced backfill misses are acceptable (usually — incremental is the backstop and the collection key collapses duplicates).

Either way, do **not** add machinery to chase an item merely _updated_ mid-sweep. A short sibling backfill completing in one in-memory run tolerates a fragile key because it rarely resumes; a multi-hour backfill resumes from persisted state constantly, so resume is the _normal_ mode.

Separate concern from the incremental cursor: incremental tails event-time with a `LogCursor`; a backfill walks a bounded set with a `PageCursor` position. Don't force one to use the other's key.

### `FETCH-CHECKPOINT-STABLE-STATE` · checkable-from-diff

**Checkpoint only what resumes stably.** Yielding a `PageCursor` asserts that everything before it is captured; whatever state it encodes must still mean the same thing after an arbitrary resume gap.

Page/offset pagination is fine in itself — but a positional offset may cross a checkpoint **only** when you can guarantee no document can have fallen out of the resumed window: membership and order under the offset are stable across the gap (a documented sort over immutable keys, or a probed-frozen row set — verify it, don't assume it). Without that guarantee, keep the offset local to a single generator invocation and checkpoint only at boundaries encoded in immutable values (e.g. the parent watermark between fully-drained parents).

The asymmetry that decides close calls: a re-read duplicate is free — the collection key collapses it — while a document skipped inside the backfill window is permanently lost, because the incremental task never revisits pre-cutoff time.

### `FETCH-DICT-CURSOR-WORKLIST` · checkable-from-diff

**When a backfill's resume state is a work list, carry it in a dict PageCursor.** `PageCursor` is `str | int | dict | None`; dict cursors (`make_cursor_dict` in `estuary_cdk.capture.common`) support JSON merge patches (RFC 7396).

Use this whenever deriving the work set is expensive (e.g. a full parent-collection drain to select units): persist the remaining set in the initial full cursor and resume from it directly — never re-derive the set per invocation, and never substitute wall-clock checkpoint-deadline machinery (`time.monotonic()` deadlines, `CHECKPOINT_INTERVAL`s) for checkpoints the cursor itself can carry. Deadline batching trades crash-loss windows for drain cost; a dict cursor eliminates both.

Dict cursors are the exception, not the default — an opaque `str` or offset `int` cursor is preferred whenever it suffices, because a bare dict's contents and meaning are hard to reconstruct later. When a dict is warranted, **document its shape with a dataclass**: a docstring stating what the keys and values are and how entries are deleted, `from_cursor_dict` to parse the persisted dict, and constructor/patch helper methods so call sites never build cursor dicts by hand. Canonical reference: `ItemsBackfillCursor` in `source-monday/source_monday/models.py`.

Mechanics that follow from the CDK contract:

- **Full state first, patches after.** The first dict cursor sets `next_page` wholesale; every later one must be a patch against it.
- **Every emitted cursor dict must pass through `make_cursor_dict`** — the marker is how the CDK distinguishes a yielded cursor from a yielded document. Route all yields through the dataclass's helpers so none can miss it. The CDK pops the marker again before persisting, so the dict handed back on resume is marker-free — unit IDs can live at the cursor's top level.
- **Deletion is a null, per JSON merge patch.** To remove an entry from the persisted cursor, yield a patch mapping its key to `None` (`{unit_id: None}` at the unit's boundary); omitting a key leaves it untouched. Have `from_cursor_dict` drop `None`-valued keys defensively.
- **Keep checkpoints small.** Store only what resume actually needs, not whole objects.
- **The runtime merges patches into its own persisted dict, which can share nested references with what you yielded** — have `from_cursor_dict` copy, and materialize your iteration order before yielding patches.
- **Never yield a `None` PageCursor.** End the backfill by returning without a trailing cursor.
- **Return after every checkpoint.** "Checkpoint per unit" means the invocation ends at the checkpoint: drain one unit, yield its patch, `return`. The runtime persists the patch and re-invokes to resume from it. Don't loop over the remaining work list inside one invocation.
- **Don't checkpoint the final unit's completion.** Yielding a patch that empties the work list persists a spent cursor and costs one extra no-op invocation just to observe it's empty; finish the last unit and return instead. Keep the empty-`remaining` early-return on the resume path as a defensive guard only.

### `FETCH-PAGE-SIZE-RESEARCH` · needs-runtime-evidence

Research and document the maximum page size **per endpoint**. Different endpoints in the same API often have different limits, and the enforcement mode (hard reject / silent clamp / silent truncation) changes the connector design. See the `bruno-probe-endpoint` skill's over-limit probe.

---

## Documentation

### `DOC-FLAG-ONLY-UNVERIFIED` · checkable-from-diff

**State facts plainly; flag only the unverified.** No "(verified live)" stamps in code comments or docstrings — verification is the default. Annotate only claims that could NOT be verified. Evidence pointers (a named Bruno probe) are fine; status stamps are not.

The Bruno collection keeps its dated `**VERIFIED (YYYY-MM-DD):**` / `**PENDING:**` / `**UNOBSERVABLE:**` markers — that's the evidence record, and it lives there, not in the code.

### `DOC-CONTRACT-NOT-MECHANISM` · checkable-from-diff

**Docstrings state the contract, not the mechanism.** Describe what the function or class guarantees its caller (invariants like "ordering is preserved across concurrent callers"), never a play-by-play of the current implementation — lock acquisition order, branch shape, exact step sequence.

Mechanisms are what refactors change, and stale explanation comments rot fastest. The test: would this comment still be true if someone restructured the body tomorrow?

### `DOC-RAIL-TIMELINE` · checkable-from-diff

**Document each window as a rail timeline** in the fetch function's docstring: one tick per named instant, one rail per query param with its bracket semantics at the queried timestamp (`(` exclusive, `]` inclusive), a bottom effect rail (`emitted` / `window`) using closed brackets on the first and last ticks actually collected, and `└─` notes anchored to the tick they explain. Boundary-semantics prose goes in one line below the graph.

Keep it hand-editable: ≤ 79 columns, no cross-rail alignment beyond the shared ticks. Copy the shape of this exemplar (from `source-mailchimp-native`'s `fetch_list_children`; adapt ticks, params, and notes to the stream — the glyph conventions are the normative part):

```
              cursor   cursor + 1s  horizon = last elapsed second
─────────────────┼──────────┼──────────────┼─────▶ time (1s ticks)
                 │          │              │
SINCE_PARAM ─────(══════════╪══════════════╪═════▶
BEFORE_PARAM ════╪══════════╪══════════════]
emitted ─────────┼──────────[══════════════]
                 │          │              └─ the present second is still
                 │          │                 in progress; its docs wait
                 │          │                 for the next poll's window
                 │          └─ first emitted second
                 └─ nothing new can appear here or earlier: this second
                    had fully elapsed when it was walked, and updates
                    always stamp "now"
```

Glyph key: `═` covered by that rail, `─` not covered; `(` / `[` / `]` sit exactly on a tick column; `╪` where a covered rail crosses a tick it doesn't bracket, `┼` where an uncovered one does; tick guideline pipes (`│`) continue through rows whose rail stops short of them; a note's continuation lines align three columns in from its `└─`; rails run top-to-bottom in request-param order with the effect rail last.

Keep notes brief — no longer than three lines in length. If more context is needed, place it below the timeline.
