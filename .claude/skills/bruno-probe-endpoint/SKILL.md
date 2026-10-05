---
name: bruno-probe-endpoint
description: Stand up a Bruno collection that hits a provider endpoint with sops-decrypted credentials, then verify the live response matches the docs before writing connector code. Use as the verification phase of `add-stream`, or any time the provider's docs need to be checked against the actual API.
argument-hint: "[provider-name] [endpoint-path]"
allowed-tools: Bash Read Write Edit Glob Grep WebFetch
---

Verify that the provider's `endpoint-path` actually returns what the docs say it does, using a Bruno collection authenticated against the connector's existing sops-encrypted credentials. The goal is to make decisions from live behavior, not docs.

## Laws

**Shared laws** — read both before Phase 1; they are the single authority and are shared with `add-stream` and `configure-auth`:

- [`.claude/shared/provider-api-consent.md`](../../shared/provider-api-consent.md) — `API-CONFIG-GATE`, `API-MUTATE-ONLY-WITH-CONSENT`, `API-ROUTE-THROUGH-BRUNO`, `API-TOKEN-EPHEMERAL`, `API-DONT-READ-CREDS`.
- [`.claude/shared/session-conduct.md`](../../shared/session-conduct.md).

**Skill-specific laws:**

1. The Bruno collection is the source of truth for "what does this endpoint actually return today." When it and the provider's docs disagree, the collection wins and the docs claim gets re-marked (Phase 5, step 4).
2. You run a mutation only when the recorded seeding answer is `seeding: assistant` and the request lives under `Seeding/`. In every other case the request is authored for the user to run, or the finding stays PENDING. `API-MUTATE-ONLY-WITH-CONSENT` states the rule; Phase 6 is the procedure.

This skill writes no connector code and never modifies `config.yaml`. Its output is a Bruno collection and a set of observed responses to compare against the docs.

## Phase 1 — Collection Layout

Create the collection **inside the connector** at `<connector>/bruno/`, in Bruno's **OpenCollection YAML** format (requires Bruno / `bru` CLI >= 3.0.0; the legacy `.bru` format is deprecated for new work):

- `opencollection.yml` — the collection root: name, collection-level auth mode, the collection-level scripts (sops pre-request auth, Phase 3; redaction post-response, Phase 5), and the collection `docs:` block. The root docs hold **collection-specific facts only** — account-wide API constraints, the index of per-endpoint LIMITATION findings, run instructions. The conventions (the marker taxonomy in [`evidence-markers.md`](../../shared/evidence-markers.md), this skill's laws) stay in the repo's `.claude/`; the root may carry a one-line pointer to them.
- `environments/<Provider>.yml` — non-secret vars (base URL, connector-relative config path).
- **One subdirectory per resource family**, each with a `folder.yml` (`info: {name, type: folder, seq}`): `Lists/`, `Members/`, `Campaigns/`, … plus underscore-prefixed folders for shared URL-shape probes (`_top-level/`, `_list-child/`) and `Seeding/` for mutations (Phase 6). One `.yml` per request inside, named for what the request proves (`bare.yml`, `offset page.yml`, `since filter.yml`) — the folder carries the resource name, so request names never repeat it.

The collection lives in the connector at `<connector>/bruno/`, committed alongside its code. If this connector already has a `bruno/` collection, extend it instead of creating a new one.

**The root docs' `## API constraints (account-wide)` block is the canonical home for cross-cutting provider limits** — it's the first thing a later session reads when surveying the API (`add-stream`'s rate-limit survey checks it before re-deriving anything). Record the published limits, any tighter per-endpoint buckets, and the documented throttling behavior there as they're established:

```markdown
## API constraints (account-wide)

- Concurrency / general: <N> req/sec per key (per provider docs, link).
- Tighter buckets:
  - `/v1/<endpoint>`: <M> req/min
  - `/v1/<events-firehose>`: <K> req/hr
- Throttling (429/403): <retry-after / exponential backoff / hard-cut>.
```

Endpoint-specific constraints do **not** go in this block (nor in `CLAUDE.md`) — they carry a `**LIMITATION**` marker on the request that proves them (see Documentation, Phase 4); the root docs keep only the index of them.

**Don't invent the layout — model it on an existing collection.** If a **sibling connector** has a `bruno/` collection, copy its structure and pre-request script from there (`source-mailchimp-native/bruno/` is a full OpenCollection YAML example). If none does, build `bruno/` from this skill's layout rules alone.

The config-path var in `environments/<Provider>.yml` can be the connector-relative `config.yaml` (e.g. `../config.yaml` from inside `bruno/`), since the collection ships with the connector.

## Phase 2 — Credential Discovery

The pre-request script needs three facts, and the connector's code already holds all of them once `configure-auth` has run. Derive them from the code; don't ask the user, and don't read the encrypted file to check its structure (`API-DONT-READ-CREDS`).

1. **Where the credentials live** — the connector's own `config.yaml`, sops-encrypted, referenced connector-relative from `bruno/`.
2. **The JSON path of the token inside the decrypted file** — the credential class in `models.py`; its field names are the path (`credentials.access_token`, `api_key`, …). Only `*_sops`-suffixed keys are encrypted, and the model field keeps the plain name.
3. **The auth scheme the provider expects** (Bearer, Basic, custom header) — the `TokenSource` settings and `api.py`.

If `models.py` has no credential class yet, a prerequisite is missing: run `configure-auth` first rather than asking the user to describe a layout that doesn't exist.

## Phase 3 — Auth Wiring

The committed pre-request script decrypts the connector's sops-encrypted `config.yaml` via Node's `child_process` and injects the credential on `req`. The same script runs in **both** runtimes — there is no fallback path:

- **CLI:** run with `bru run … --sandbox developer`.
- **GUI (Bruno desktop):** the collection's JS sandbox must be set to **developer** mode (`jsSandboxMode: "developer"` in `~/.config/bruno/collection-security.json` — see [[reference-bruno-sandbox-mode-location]]). In developer mode the GUI sandbox exposes Node built-ins, so the identical `child_process` + `sops` path works there too. No keychain / `vars:secret` fallback is needed.

Two house styles, by what the decrypted config holds: **A**, a static token attached directly (live reference: `source-mailchimp-native/bruno/opencollection.yml`); **B**, an OAuth refresh token exchanged for an access token on every request. The script contract and the style B block are in [`auth-wiring.md`](auth-wiring.md); copy style A from the sibling collection, which is always more current than a doc.

## Phase 4 — Request Authoring

Author the smallest set that proves the endpoint behaves as the docs say. Typically:

- The bare list (or bare GET).
- The list with the cursor/filter the connector will actually use (e.g. `created[gte]=…`).
- If the connector uses event-based incremental, the related events query.

Pin specific ids (e.g. for a fetch-by-id request) from the bare list's response — define parameters and fill them in yourself.

Keep `environments/<Provider>.yml` free of dead vars: it should hold only vars some request references via `{{…}}` (base URL, config path, pinned ids). Unset values use a self-describing `PIN_…` placeholder (e.g. `PIN_FROM_LISTS_BARE`) so an unpinned run fails loudly rather than silently hitting a malformed URL.

**Order the tree with folders, not seq bands.** Each resource family's folder carries its own `info.seq`; within a folder, requests run `seq` 1..n, ordered `bare` first, then filters/cursors, then the mechanical pagination probes. The folder split is what keeps the GUI tree skimmable as the collection accretes streams across sessions — per-session incremental numbering over a flat top level scrambles it.

**Chain dependent requests with scripts, not run order.** When request B consumes an id produced by request A (seed step 2 needs the campaign id created by step 1, a fetch-by-id needs the bare list's first id), don't leave a `PIN_FROM_…` placeholder for a human to fill. Give A an after-response script (`runtime.scripts`, `type: after-response`) that captures the id into a runtime var (`bru.setVar('seed_campaign_id', res.body.id)`), reference `{{seed_campaign_id}}` in B, and give B a before-request guard that runs A when the var is missing: `if (!bru.getVar('seed_campaign_id')) await bru.runRequest('Seeding/Create Campaign');` (path from the collection root, no extension). B is then self-contained: run alone it pulls in its prerequisite; run after A (GUI, or a folder-level `bru run`) the guard sees the var and doesn't run A twice. Runtime vars are in-memory only, so this is fine for resource ids — never for credentials (`API-TOKEN-EPHEMERAL`). Don't use a declarative pre-request var default for the fallback — it is reassigned on every run and would clobber the value A captured — and never call `bru.runRequest` from a collection-level script, which recurses.

If the upstream `add-stream` skill's rate limit survey flagged a tight per-endpoint budget, keep the request set minimal — typically the bare list and one filtered list is enough to confirm shape and cursor behavior.

### Cursors

Name the cursor fields the connector will use, and prove they work. Before authoring requests, decide which document field(s) drive backfill (date-range or sequence cursor on the list endpoint) and incremental (events `created`/`updated_at`, monotonic sequence, etc.). State them explicitly — type (int unix-second, RFC3339 string, opaque token), source object, and filter parameter name. Then author at least one request that exercises each cursor with a non-trivial filter value, and confirm in Phase 5 that the filter is honored (response narrows as expected, pagination contract holds). If backfill and incremental use the same cursor source, say so; if they don't, call out the handoff and verify the units align (e.g. both unix seconds — no millisecond/RFC3339 drift across the cutoff). For RFC3339 string cursors, write every request-side timestamp in one canonical form — UTC `Z` (e.g. `2026-06-10T00:00:00Z`), matching what the connector emits — so probes stay diffable across the collection; leave response-body timestamps exactly as the API returned them.

**Opaque-token cursors require stability evidence.** If a cursor is an opaque server-issued token (continuation token, `next_page_token`, base64-encoded blob, "sync token", etc.) rather than a value derived from the document itself, you must produce documentation proving the token is **durable across time and connector restarts** — specifically: (a) it does not expire on a wall-clock TTL, (b) it is not invalidated by unrelated provider-side state changes, and (c) it can be resumed days or weeks after issue. Link the relevant provider doc paragraph in the verification report (URL + the exact quoted line). Docs silent or ambiguous → `GATE-OPAQUE-CURSOR` ([`interaction-mode.md`](../../shared/interaction-mode.md)): a connector that checkpoints an opaque token of unspecified lifetime is a latent data-loss bug, so human-in-the-loop surfaces it as a blocker for the user's call, and autonomous rejects the opaque cursor in favor of a derived cursor or a snapshot and ledgers it. Derived cursors (timestamps, monotonic ids) are exempt from this rule because their stability is implied by the data model.

### Silent Data Loss

Audit every list endpoint's default filters for silent data loss. Providers often default to "show me the live, non-deleted, non-archived subset" — e.g. Stripe's `/v1/prices` defaults to `active=true`, `/v1/subscriptions` defaults to `status=active`. Before declaring an endpoint verified, read the docs for **every** parameter that has a default and ask: does the default exclude documents the connector is supposed to capture? In particular check for `active`, `status`, `state`, `deleted`, `archived`, `visibility`, and similar partition-by-state parameters. If the default excludes a partition:

- Quote the doc line that establishes the default in the verification report.
- Run a request for each excluded partition (e.g. `active=false`) and save it as a saved example — even if the test account returns zero items for that partition, you've proved the filter is honored and documented the request shape for production accounts that _do_ have data there.
- `GATE-PARTITION-COVERAGE` ([`interaction-mode.md`](../../shared/interaction-mode.md)): how the connector covers the full population — one sweep with a sentinel value (`status=all`) when the provider offers one, sequential sweeps per partition, or per-partition subtasks with independent cursors. Each choice changes the cursor topology. Human-in-the-loop: decide with the user. Autonomous: sentinel when offered, otherwise sequential sweeps (the simplest topology that observes every row); ledger it. Record the decision in the report either way.

### Documented limits

Push every documented limit one past its stated maximum and record what the provider actually does — don't take the doc's word for the ceiling. If the docs say `count` max is 1000, author a probe with `count=1001` (and apply the same +1 to any other ceiling the connector leans on: page size, max ids per request, batch size). The point is to learn the **enforcement mode**, which the docs rarely state and which changes the connector design:

- **Rejected (4xx)** — the provider errors on over-limit requests; the connector must clamp to the max itself before sending.
- **Silent clamp** — the provider quietly serves the max (asked 1001, returned 1000, `total_items` unchanged); benign, but confirm it's a clamp and not truncation.
- **Silent truncation / data loss** — the provider returns the max and drops the remainder _without_ a pagination signal; a connector that trusts the requested count would lose data. This is the case the probe exists to catch.

Save the over-limit response as an example named for the observed behavior (e.g. `400 — count 1001 rejected`, `200 — count 1001 clamped to 1000`), so the enforcement mode is recorded evidence, not an assumption.

**But never add obscure machinery to chase a limit — clarity beats coverage.** The cheap `+1` probe is a single clean request: always worth it. Distinguishing _clamp vs. silent truncation_, though, needs more items than the cap (e.g. >1000 records in one collection). Only seed that far if it's reachable cleanly — a real batch endpoint, or an endpoint where a handful of visible `POST` runs gets there (keep the verb visible per Phase 6). If reaching it would require a scripted bulk-seeder, a request-generating loop, or a mutation with no clean cleanup (no bulk-delete endpoint), **don't** — note in the probe docs that we trust the docs for the boundary behavior and why it's safe (e.g. offset/count + short-page completion recovers all items regardless of clamping), and move on.

### Pagination order & cap truncation

Two related correctness properties for a sorted, paged list:

- **The cap truncates the sort _tail_, not the middle.** When a sorted result is capped below the number of matching rows — the endpoint's documented max is 1000 and 1010 rows match under `sort=updated_at ASC` — the 1000 returned must be the _first_ 1000 by the sort key, and the 10 omitted must be exactly the sort-order tail (the most-recently-updated). If the cap drops an arbitrary middle row instead, a connector resuming from `offset=1000` (or `cursor = max updated_at seen`) never re-fetches it → silent data loss. **Observing this directly requires more rows than the cap** — usually bulk-seeding. If that's the obscure machinery you've ruled out (clarity over coverage — see "Documented limits"), trust the provider's documented sorted-pagination contract for the cap and **record the assumption** in the over-limit probe's docs rather than leaving it implied.
- **Ordered, contiguous paging below the cap.** What you _can_ check cheaply: a `count=1` walk over offsets `0..N` reassembles the full set (same ids, same order, no gap/dupe/reorder). This exercises the same pagination machinery below the cap and is worth doing — but note it does **not** prove the hard cap truncates in order. Matters most on endpoints with **no sort parameter**, where paging relies on a stable default order; prove that stability rather than assume it.

### Documentation

**Every request links to its provider reference URL.** Put the canonical docs URL for that exact endpoint in the request's `docs:` block as a `**REFERENCE:**` line (first line of the block). The URL must point at the specific endpoint page — not the provider's API index — so a reviewer can click straight from the request to the spec it was verified against. If the provider versions its docs, prefer a versioned URL over a `/latest/` redirect. **Never write a `**REFERENCE:**` URL — or any "the docs say X" claim — from recall:** fetch the page first, confirm it's live and actually says X, and only then cite it. Recalled URLs 404 and recalled claims drift, and reading the real page surfaces load-bearing facts recall missed.

**Every request has a `**WHY:**` section** — why the request exists and what we're supposed to learn from it. A reader must be able to tell, without running anything, what question the request was built to answer and what the connector design does with the answer. Seeding requests state what downstream probe their data feeds.

**When a docs block names another request, write it folder-qualified as `` `Folder / info.name` ``** (e.g. ``see `Members / since boundary probe` ``; ``run `Seeding/B2 - Add Tagged Member 2` first``) so a reader can find it — bare request names repeat across folders. After renaming or moving any request, grep the collection for backtick-quoted references to the old name and update them, remembering that references wrapped across a line break won't match a whole-name grep — a docs block pointing at a name that no longer exists is a stale string a reviewer can't resolve.

**Tag every behavioral claim with its evidence marker when you draft it**, per [`evidence-markers.md`](../../shared/evidence-markers.md): VERIFIED, DOCUMENTED, PENDING or UNOBSERVABLE, plus LIMITATION on endpoint-specific constraints with connector consequences. That file also holds the hand-off rule for PENDING claims.

## Phase 5 — Execution

For each request:

1. `bru run "<request>.yml" --env <Provider> --sandbox developer` from the collection directory (requires `bru` CLI >= 3.0.0 for the OpenCollection YAML format). The repo's hook refuses the run while `config.yaml` is dirty or unstamped (`GATE-CONFIG-DIRTY`); if it does, follow its message.
2. **Save every successful response as a Bruno response example.** Bruno's native "Response Examples" feature (see [docs](https://docs.usebruno.com/send-requests/res-data-cookies/response-examples.md)) persists a response as an entry in the request's `examples:` list, inside the same `.yml`. After each run, save the response with a descriptive name (e.g. `200 — populated list`, `200 — empty list`, `400 — invalid filter`) so future runs can be diffed against the recorded shape and future stream additions inherit the artifact.
   - **Mirror the connector's snapshot redactions.** Whatever fields the connector's snapshot tests redact or ignore (HATEOAS `_links`, volatile URLs/ids, churning timestamps), redact the _same_ fields in saved examples with the _same_ marker (e.g. `"REDACTED"`). The example should reflect what the tests actually assert, not the raw response — otherwise the two drift and a reviewer can't tell signal from noise. **Mechanism:** prefer a collection-level after-response script in `opencollection.yml` (`request.scripts`, `type: after-response`) that walks `res.getBody()` and `res.setBody(...)` with the redactions — verified to apply to the saved response example, so it's automatic and there's no manual edit step to forget. (Guard it to JSON object/array bodies; never parse throttle-error bodies.) Two caveats: it's a _second_ copy of the field list (the first being the test's `REDACTED_FIELDS`), so comment each pointing at the other and keep them in sync; and it hides those fields from live `bru run` output too, so keep the list **minimal** — only fields you never need to eyeball during verification.
   - **Body proportionality.** Full provider objects are often mostly boilerplate (HATEOAS `_links`, unused settings blocks). The `bare`/shape request keeps one full representative item; filter/pagination/finding probes save a projection of just the decision-relevant fields (id, the cursor field, `total_items`), and note the projection in the example name so it isn't mistaken for the full response.
3. Diff the response shape against the provider docs: key set on the object envelope (`object`, `has_more`, `data`), per-item key set, id prefix, pagination contract. Note discrepancies before proceeding.
4. **Reconcile the request's own `docs:` against the evidence you just saved.** A request's docs are drafted from a pre-run hypothesis ("the `name` filter is substring; expect narrowing"); the live response is the verdict. Re-read the docs and rewrite any claim the result contradicted so the prose states the _observed finding_ as a dated `**VERIFIED (YYYY-MM-DD):**` entry, never the refuted guess — the docs block and its saved example must never disagree. This collection is a memory layer for future readers, human and assistant; a docs block recording a refuted hypothesis is worse than an empty one. If you could **not** capture an example for a request (verified in a sibling session, or it needs a mutation you handed off in Phase 6), mark the claim `**PENDING:** <why + how to close>`; if no practical path to evidence exists (feature-gated, unseedable), mark it `**UNOBSERVABLE:** <reason + what you rely on instead>` (see Documentation, Phase 4).

Surface findings as a short report — what matched, what didn't, what the implications are for the connector implementation. The report must include a **Cursors** section naming the chosen backfill cursor, the chosen incremental cursor, their types and filter parameters, and a one-line note confirming the filter behavior observed in the live runs (e.g. "`created[gte]=… → narrowed from 9 to 3 items, has_more=true`").

## Phase 6 — Mutation & Seeding

Some verification needs provider state to change. Either an incremental cursor can only be confirmed by provoking an `update` event, or a read endpoint is empty and its shape, pagination, and cursor behavior can't be observed until records exist. Both are handled the same way: the mutation becomes a request under `Seeding/`, and who runs it is `GATE-SEEDING`, settled by the seeding answer in your brief (`API-MUTATE-ONLY-WITH-CONSENT`). Decide that first, because it also decides whether to author the request at all.

### Who runs it

A seeding request exists to be run in this session, so author one only when someone in this session will run it:

- **`seeding: assistant`**, either mode — author the request and run it yourself against the sandbox. This is the only case in which you mutate, and only for requests under `Seeding/`.
- **`seeding: user`**, human-in-the-loop mode — author the request and hand it over now; resume only after the user confirms they ran it.
- **Anything else** — `seeding: user` in autonomous mode, `seeding: none`, or no answer — author nothing beyond what proves the gap. Nobody will run the request in this session, and an unrun seeding request is dead weight. Record in the verification report what the endpoint needs and move on.

In every branch the finding the mutation would unblock stays `**PENDING:**` (stating what seeding would resolve it) until the request has actually run. Relabeling it UNOBSERVABLE to clear the finalize check misstates the evidence.

### What to author

**Provoking a change (incremental verification).** Write one request for the mutation:

- Pre-fill it with a specific id from a list response.
- Target a no-op-shaped field (typically `metadata` or `nickname`) so other endpoints' snapshots aren't perturbed.

**Seeding an empty endpoint.** Write the create requests (POST/PUT) for a small, representative set of test records:

- One request per object type the empty endpoint depends on, in dependency order (e.g. create a customer before an invoice).
- Use minimal, clearly-labeled test values; parent ids come from the chained request's runtime var, never from pinned literals.
- Chain each step to the one it depends on (see "Chain dependent requests" in Phase 4) so any request in the sequence can be run alone and pulls in its prerequisites.
- Record in the verification report that the endpoint was empty and what seeding it needs, so the gap is visible even before the requests run.

### How to author it

**Placement.** Every mutating/seeding request lives in a `Seeding/` subdirectory of the collection — never at the collection's top level. The top level holds only read-only verification requests; the folder split keeps the mutating set visually distinct and lets the whole sequence run with a folder-level `bru run`.

**Dependencies live in the request, not its name.** Name a seeding request for the mutation it performs (`Create Customer.yml`, `Archive Member.yml`) — no run-order prefixes. Ordering is enforced by the chaining guard from Phase 4: a request that needs a seeded resource `bru.runRequest`s the request that seeds it when the runtime var is missing, so the reader sees its prerequisites in the script and can run any request alone. Post-read mutations that touch already-seeded records (an archive, an update that advances a cursor) chain to the request that seeded the record the same way. Still order `info.seq` so every request comes after the ones it chains to: a folder-level `bru run` walks seq order, and a dependent listed first would seed its prerequisite itself and then the runner would seed it again. Collections written before this rule carry `A1 - …` prefixes and pinned-id fallbacks; treat that as legacy, don't copy it.

**Keep the mutation's HTTP verb visible — never hide it inside a decoy GET.** A seeding request's method and URL must be the mutation it performs (`post { url: …/lists/{id} }`), so a reviewer sees what it does at a glance. Do **not** make the request a `GET` whose pre-request script secretly POSTs/DELETEs — that obscures the real call.

**Never loop requests in a script, and cap seeding at ~10 manual requests.** No `bru.sendRequest` loops, no script-driven request generation, no "advance an offset env var and re-run N times" schemes. A seeding script may shape a _single_ request's body, but it must not stand in for many requests. The hard line: **if a precondition can't be set up in ~10 plain, visible, individually-runnable requests, drop the request entirely** — don't author the read probe it would feed. Verifications that need bulk data (e.g. exceeding a page cap to watch its truncation order) are simply out of scope; trust the docs and record the assumption (see "Pagination order & cap truncation" and "Documented limits").

**Docs block.** A seeding request gets a `docs:` block like any other request, holding what is true of the request itself:

- `**WHY:**` — which downstream probe the seeded data feeds.
- **Prerequisites** — anything the chaining guard can't set up (account-level settings, a sandbox flag), and how to clean up afterwards.
- **Outcome** — added once the request has actually run: a dated line such as "Outcome: ran YYYY-MM-DD; finding VERIFIED in `<request>`".

### After it runs

Whoever ran it, re-run the affected read request to capture the now-populated example, add the **Outcome** line to the seeding request's docs, and resolve the PENDING finding to `**VERIFIED (YYYY-MM-DD):**` (Phase 5, step 4).
