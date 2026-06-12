---
name: bruno-probe-endpoint
description: Stand up a Bruno collection that hits a provider endpoint with sops-decrypted credentials, then verify the live response matches the docs before writing connector code. Use as the verification phase of `add-stream`, or any time the provider's docs need to be checked against the actual API.
argument-hint: "[provider-name] [endpoint-path]"
allowed-tools: Bash Read Write Edit Glob WebFetch
---

Verify that the provider's `endpoint-path` actually returns what the docs say it does, using a Bruno collection authenticated against the connector's existing sops-encrypted credentials. The goal is to make decisions from live behavior, not docs.

## Laws

These apply in every phase. Re-read them before each phase boundary.

1. Verify first, code second. The Bruno collection is the source of truth for "what does this endpoint actually return today."
2. **API-call rule:**
   - **Read-only calls (GET, HEAD):** allowed without per-call consent **iff** `git status --porcelain <connector>/config.yaml` is empty AND the file is tracked. Check this before _every_ read call, not just the first.
   - **`config.yaml` shows any uncommitted modification:** stop and ask before the next call, every time. The user may have repointed the credentials at a different account (prod vs. test) — never assume.
   - **Effectful calls (POST/PUT/PATCH/DELETE, or any GET known to have side effects like cursor consumption):** _always_ ask, regardless of `config.yaml` state. Don't run them yourself even after consent — hand the `.bru` to the user (see Phase 6).
3. The decrypted token is held only on `req` for the in-flight request. Never `bru.setVar`, never `bru.setEnvVar`, never write it to a committed file — it is re-derived from `sops` on every request, in both CLI and GUI.
4. Don't read the encrypted credentials file directly. Ask the user where it lives and what JSON path the token sits at.
5. Never mutate provider state. If verification needs a mutation, write the `.bru` and hand it to the user — do not run it yourself.
6. Open the session with a TODO list. One task per phase, plus any per-endpoint follow-ups.

## Phase 0 — Stance

Make explicit to the user what this skill does _not_ do: it does not write connector code, does not modify `config.yaml`, does not run any effectful API calls. It produces a Bruno collection and a set of observed responses for you to compare against the docs.

## Phase 1 — Collection Layout

Create the collection **inside the connector** at `<connector>/bruno/` with:

- `bruno.json` — collection metadata.
- `collection.bru` — collection-level headers, auth mode, and the sops pre-request script (Phase 3).
- `environments/<Provider>.bru` — non-secret vars (base URL, connector-relative config path).
- One `.bru` per request you author in Phase 4.

The collection lives in the connector at `<connector>/bruno/`, committed alongside its code. If this connector already has a `bruno/` collection, extend it instead of creating a new one.

**Don't invent the layout — model it on an existing collection.** To find a reference:

1. If a **sibling connector** has a `bruno/` collection, copy its structure and pre-request script from there.
2. Otherwise, use the user's **Bruno workspace** (the directory where their reference collections live). If its location isn't already recorded in memory ([[reference-bruno-workspace]]), **ask the user where their Bruno workspace is and commit the answer to memory** so the next run doesn't ask again. Then model on a collection found there.

The config-path var in `environments/<Provider>.bru` can be the connector-relative `config.yaml` (e.g. `../config.yaml` from inside `bruno/`), since the collection ships with the connector.

## Phase 2 — Credential Discovery

Ask the user:

1. Where do the connector's credentials live? (Typically `<connector>/config.yaml`, sops-encrypted.)
2. What's the JSON path of the access token inside the decrypted file? (Names vary — `credentials.access_token`, `credentials.access_token_sops`, `api_key`, etc.)
3. What auth scheme does the provider expect? (Bearer, Basic, custom header.) Confirm by reading the connector's existing `api.py` — don't guess.

Do not read the encrypted file just to check structure.

## Phase 3 — Auth Wiring

The committed pre-request script decrypts the connector's sops-encrypted `config.yaml` via Node's `child_process` and injects the credential on `req`. The same script runs in **both** runtimes — there is no fallback path:

- **CLI:** run with `bru run … --sandbox developer`.
- **GUI (Bruno desktop):** the collection's JS sandbox must be set to **developer** mode (`jsSandboxMode: "developer"` in `~/.config/bruno/collection-security.json` — see [[reference-bruno-sandbox-mode-location]]). In developer mode the GUI sandbox exposes Node built-ins, so the identical `child_process` + `sops` path works there too. No keychain / `vars:secret` fallback is needed.

Implementation contract:

- Use `execFileSync('sops', ['-d', '--output-type=json', configPath])` (argv-form — avoids shell injection if the config path ever contains spaces or special chars). Let real sops failures (missing file, bad path, missing field) throw loudly; don't swallow them.
- The decrypted token lives on `req` only. Never `bru.setEnvVar(…)` it back, even temporarily — that would persist it to the environment file and into git (Law #3).
- The JSON paths below (`credentials.access_token`, etc.) are illustrative — use the exact path Phase 2 established for this connector.

### House style A — provider takes a static token (e.g. Stripe)

When the sops-decrypted config exposes a long-lived API key directly, decrypt and attach in one step. Reference: a static-token collection's `collection.bru` (e.g. the Stripe collection in your Bruno workspace).

```js
script:pre-request {
  const { execFileSync } = require('child_process');
  const configPath = bru.getEnvVar('config_path');
  if (!configPath) throw new Error('config_path env var is not set');
  const decrypted = execFileSync(
    'sops', ['-d', '--output-type=json', configPath], { encoding: 'utf8' },
  );
  const token = JSON.parse(decrypted)?.credentials?.access_token;
  if (!token) throw new Error('credentials.access_token missing from decrypted config');
  req.setHeader('Authorization', `Bearer ${token}`);
}
```

### House style B — provider uses OAuth refresh-token (e.g. HubSpot)

When the sops-decrypted config holds a `refresh_token` plus `client_id`/`client_secret`, exchange them for a short-lived access token via `bru.sendRequest` on every request. Reference: an OAuth refresh-token collection's `collection.bru` (e.g. the HubSpot collection in your Bruno workspace).

```js
script:pre-request {
  const { execFileSync } = require('child_process');
  const configPath = bru.getEnvVar('config_path');
  if (!configPath) throw new Error('config_path env var is not set');
  const decrypted = execFileSync(
    'sops', ['-d', '--output-type=json', configPath], { encoding: 'utf8' },
  );
  const creds = JSON.parse(decrypted)?.credentials;
  if (!creds?.client_id || !creds?.client_secret || !creds?.refresh_token) {
    throw new Error('credentials.{client_id,client_secret,refresh_token} missing');
  }

  const resp = await bru.sendRequest({
    method: 'POST',
    url: 'https://api.hubapi.com/oauth/v1/token',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    data:
      'grant_type=refresh_token' +
      `&client_id=${encodeURIComponent(creds.client_id)}` +
      `&client_secret=${encodeURIComponent(creds.client_secret)}` +
      `&refresh_token=${encodeURIComponent(creds.refresh_token)}`,
  });
  const accessToken = resp?.data?.access_token;
  if (!accessToken) throw new Error(`token refresh failed: ${JSON.stringify(resp?.data ?? resp)}`);
  req.setHeader('Authorization', `Bearer ${accessToken}`);
}
```

The per-request OAuth roundtrip is intentional — caching the access token via `bru.setEnvVar` would persist it to a committed file (Law #3 violation). Most providers' token endpoints are far above the connector's rate-limit budget; if yours isn't, that's a separate problem to surface to the user.

When pasting either style into a new collection, look at a reference `collection.bru` — a sibling connector's `bruno/collection.bru`, or a reference collection in your Bruno workspace (e.g. Stripe) — for the most current shape rather than copying from this doc.

## Phase 4 — Request Authoring

Author the smallest set that proves the endpoint behaves as the docs say. Typically:

- The bare list (or bare GET).
- The list with the cursor/filter the connector will actually use (e.g. `created[gte]=…`).
- If the connector uses event-based incremental, the related events query.

Pin specific ids (e.g. for a fetch-by-id request) from the bare list's response — define parameters and fill them in yourself.

Keep `environments/<Provider>.bru` free of dead vars: it should hold only vars some request references via `{{…}}` (base URL, config path, pinned ids). Unset values use a self-describing `PIN_…` placeholder (e.g. `PIN_FROM_LISTS_BARE`) so an unpinned run fails loudly rather than silently hitting a malformed URL.

**Band `meta.seq` by resource family** so the GUI tree stays skimmable as the collection accretes streams across sessions: give each resource a block (e.g. shared probes 1–9, Lists 10–19, Campaigns 20–29, Members 40–49), ordered `bare` first, then filters/cursors, then the mechanical pagination probes. Plain per-session incremental numbering scrambles the tree once several streams land from parallel sessions.

**Chain sequential requests with post-response scripts.** When request B consumes an id produced by request A (seed step 2 needs the campaign id created by step 1, a fetch-by-id needs the bare list's first id), don't leave a `PIN_FROM_…` placeholder for a human to fill — add a `script:post-response` to A that captures the id into a runtime var (`bru.setVar('seed_campaign_id', res.body.id)`) and reference `{{seed_campaign_id}}` in B. The user can then run the sequence seamlessly, in the GUI or via a folder-level `bru run`. Runtime vars are in-memory only, so this is fine for resource ids — never do it for credentials (Law #3). For out-of-order runs, give B a _conditional_ fallback in `script:pre-request` (`if (!bru.getVar('seed_campaign_id')) bru.setVar('seed_campaign_id', '<pinned id>')`) — don't use a `vars:pre-request` default, which is reassigned on every run and would clobber the value A captured — and note in B's `docs { }` that A must run first in a fresh session.

If the upstream `add-stream` skill's rate limit survey flagged a tight per-endpoint budget, keep the request set minimal — typically the bare list and one filtered list is enough to confirm shape and cursor behavior.

### Cursors

Name the cursor fields the connector will use, and prove they work. Before authoring requests, decide which document field(s) drive backfill (date-range or sequence cursor on the list endpoint) and incremental (events `created`/`updated_at`, monotonic sequence, etc.). State them explicitly — type (int unix-second, RFC3339 string, opaque token), source object, and filter parameter name. Then author at least one `.bru` that exercises each cursor with a non-trivial filter value, and confirm in Phase 5 that the filter is honored (response narrows as expected, pagination contract holds). If backfill and incremental use the same cursor source, say so; if they don't, call out the handoff and verify the units align (e.g. both unix seconds — no millisecond/RFC3339 drift across the cutoff). For RFC3339 string cursors, write every request-side timestamp in one canonical form — UTC `Z` (e.g. `2026-06-10T00:00:00Z`), matching what the connector emits — so probes stay diffable across the collection; leave response-body timestamps exactly as the API returned them.

**Opaque-token cursors require stability evidence.** If a cursor is an opaque server-issued token (continuation token, `next_page_token`, base64-encoded blob, "sync token", etc.) rather than a value derived from the document itself, you must produce documentation proving the token is **durable across time and connector restarts** — specifically: (a) it does not expire on a wall-clock TTL, (b) it is not invalidated by unrelated provider-side state changes, and (c) it can be resumed days or weeks after issue. Link the relevant provider doc paragraph in the verification report (URL + the exact quoted line), and if the docs are silent or ambiguous, treat that as a **blocker** — surface it to the user. A connector that checkpoints an opaque token whose lifetime is unspecified is a latent data-loss bug. Derived cursors (timestamps, monotonic ids) are exempt from this rule because their stability is implied by the data model.

### Silent Data Loss

Audit every list endpoint's default filters for silent data loss. Providers often default to "show me the live, non-deleted, non-archived subset" — e.g. Stripe's `/v1/prices` defaults to `active=true`, `/v1/subscriptions` defaults to `status=active`. Before declaring an endpoint verified, read the docs for **every** parameter that has a default and ask: does the default exclude documents the connector is supposed to capture? In particular check for `active`, `status`, `state`, `deleted`, `archived`, `visibility`, and similar partition-by-state parameters. If the default excludes a partition:

- Quote the doc line that establishes the default in the verification report.
- Run a `.bru` for each excluded partition (e.g. `active=false`) and save it as a saved example — even if the test account returns zero items for that partition, you've proved the filter is honored and documented the request shape for production accounts that _do_ have data there.
- Decide with the user how the connector will cover the full population — single sweep with a sentinel value (`status=all`) when the provider offers one, multiple sequential sweeps, or per-partition subtasks with independent cursors. Each choice has different implications for cursor topology; record the decision in the report.

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

**Every `.bru` request links to its provider reference URL.** Put the canonical docs URL for that exact endpoint in the request's `docs { }` block as a `Reference: <url>` line (first line of the block). The URL must point at the specific endpoint page — not the provider's API index — so a reviewer can click straight from the request to the spec it was verified against. If the provider versions its docs, prefer a versioned URL over a `/latest/` redirect.

**When a docs block names another request, use that request's exact `meta.name`** (e.g. ``run `B2 - Add Tagged Member 2` first``) so a reader can find it. After renaming any request, grep the collection for backtick-quoted references to the old name and update them — a docs block pointing at a name that no longer exists is a stale string a reviewer can't resolve.

## Phase 5 — Execution

Apply Law #2 to every request. For each:

1. `git status --porcelain <connector>/config.yaml` — empty? proceed. Non-empty? ask.
2. `bru run "<request>.bru" --env <Provider> --sandbox developer` from the collection directory.
3. **Save every successful response as a Bruno response example.** Bruno's native "Response Examples" feature (see [docs](https://docs.usebruno.com/send-requests/res-data-cookies/response-examples.md)) persists a response as a nested sub-request under the main `.bru`. After each run, save the response with a descriptive name (e.g. `200 — populated list`, `200 — empty list`, `400 — invalid filter`) so future runs can be diffed against the recorded shape and future stream additions inherit the artifact. The first time you do this in a collection, inspect the resulting on-disk layout (likely a sibling file or folder) so the skill output is reproducible.
   - **Mirror the connector's snapshot redactions.** Whatever fields the connector's snapshot tests redact or ignore (HATEOAS `_links`, volatile URLs/ids, churning timestamps), redact the _same_ fields in saved examples with the _same_ marker (e.g. `"REDACTED"`). The example should reflect what the tests actually assert, not the raw response — otherwise the two drift and a reviewer can't tell signal from noise. **Mechanism:** prefer a collection-level `script:post-response` in `collection.bru` that walks `res.getBody()` and `res.setBody(...)` with the redactions — verified to apply to the saved Response Example, so it's automatic and there's no manual edit step to forget. (Guard it to JSON object/array bodies; never parse throttle-error bodies.) Two caveats: it's a _second_ copy of the field list (the first being the test's `REDACTED_FIELDS`), so comment each pointing at the other and keep them in sync; and it hides those fields from live `bru run` output too, so keep the list **minimal** — only fields you never need to eyeball during verification.
   - **Body proportionality.** Full provider objects are often mostly boilerplate (HATEOAS `_links`, unused settings blocks). The `bare`/shape request keeps one full representative item; filter/pagination/finding probes save a projection of just the decision-relevant fields (id, the cursor field, `total_items`), and note the projection in the example name so it isn't mistaken for the full response.
4. Diff the response shape against the provider docs: key set on the object envelope (`object`, `has_more`, `data`), per-item key set, id prefix, pagination contract. Note discrepancies before proceeding.
5. **Reconcile the request's own `docs { }` against the evidence you just saved.** A request's docs are drafted from a pre-run hypothesis ("the `name` filter is substring; expect narrowing"); the live response is the verdict. Re-read the docs and rewrite any claim the result contradicted so the prose states the _observed finding_, never the refuted guess — the docs block and its saved example must never disagree. This collection is a memory layer for future readers, human and assistant; a docs block recording a refuted hypothesis is worse than an empty one. If you could **not** capture an example for a request (feature-gated, verified in a sibling session, or it needs a mutation you handed off in Phase 6), mark its docs `UNVERIFIED: <why>` so a reader can tell evidence from hypothesis.

Surface findings as a short report — what matched, what didn't, what the implications are for the connector implementation. The report must include a **Cursors** section naming the chosen backfill cursor, the chosen incremental cursor, their types and filter parameters, and a one-line note confirming the filter behavior observed in the live runs (e.g. "`created[gte]=… → narrowed from 9 to 3 items, has_more=true`").

## Phase 6 — Mutation & Seeding Handoff

Some verification needs provider state to change. There are two cases; in **both**, you _author_ the `.bru` and hand it to the user — you never run a state-mutating call yourself (Law #5).

**Placement:** every mutating/seeding `.bru` lives in a `Seeding/` subdirectory of the collection — never at the collection's top level. The top level holds only read-only verification requests; the folder split keeps the mutating set visually distinct and lets the user run the whole sequence with a folder-level `bru run`.

**Naming:** seeding requests that run in sequence are named with a `[LETTER][NUMBER]` prefix — one letter per dependency group, numbered in run order within the group (e.g. `A1 - Create Customer.bru`, `A2 - Create Invoice.bru`; an unrelated sequence gets `B1`…`B4`). The letter tells the user at a glance which requests form one sequence (and share chained runtime vars — see "Chain sequential requests" in Phase 4); the number is the order to run them in. A mutation that depends on a seeded resource is **not** standalone — it belongs to that resource's dependency group, or to a trailing group (`D1`, `D2`, …) for post-read mutations that touch already-seeded records (an archive, an update that advances a cursor). Reserve no-prefix only for a mutation that depends on nothing and that nothing depends on. Keep `meta.seq` consistent with the prefix ordering and contiguous within `Seeding/`, so the GUI lists the whole sequence in run order.

**Provoking a change (incremental verification).** If confirming an incremental cursor requires an `update` event, write a `.bru` for the mutation:

- Pre-fill it with a specific id from a list response.
- Target a no-op-shaped field (typically `metadata` or `nickname`) so other endpoints' snapshots aren't perturbed.

**Seeding when a read endpoint is empty.** If a `list`/`GET` returns no records — so you can't observe the real response shape, pagination, or cursor behavior — author the create requests (POST/PUT) needed to populate a small, representative set of test records:

- One `.bru` per object type the empty endpoint depends on, in dependency order (e.g. create a customer before an invoice).
- Use minimal, clearly-labeled test values; pin any parent ids from earlier responses.
- Wire each step's output into the next step's input with post-response scripts (see "Chain sequential requests" in Phase 4) so the user can run the whole seeding sequence without hand-copying ids between requests.
- Record in the verification report that the endpoint was empty and what seeding it needs, so the gap is visible even before the user runs the requests.

**Keep the mutation's HTTP verb visible — never hide it inside a decoy GET.** A seeding request's method and URL must be the mutation it performs (`post { url: …/lists/{id} }`), so a reviewer sees what it does at a glance. Do **not** make the request a `GET` whose pre-request script secretly POSTs/DELETEs — that obscures the real call.

**Never loop requests in a script, and cap seeding at ~10 manual requests.** No `bru.sendRequest` loops, no script-driven request generation, no "advance an offset env var and re-run N times" schemes. A seeding script may shape a _single_ request's body, but it must not stand in for many requests. The hard line: **if a precondition can't be set up in ~10 plain, visible, individually-runnable requests, drop the request entirely** — don't author the read probe it would feed. Verifications that need bulk data (e.g. exceeding a page cap to watch its truncation order) are simply out of scope; trust the docs and record the assumption (see "Pagination order & cap truncation" and "Documented limits").

For either case, add a `docs { }` block stating: only the user runs this; never the assistant. Hand the request(s) to the user and resume only after they confirm they've run them — then re-run the affected read `.bru` to capture the now-populated example.
