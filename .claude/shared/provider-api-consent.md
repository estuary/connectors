# Provider API calls: consent, budget, and credentials

### `API-CONFIG-GATE` · conduct-only

**Read-only calls (GET, HEAD):** allowed without per-call consent **iff** `git status --porcelain <connector>/config.yaml` is empty AND the file is tracked. Check this before _every_ read call, not just the first.

**If `config.yaml` shows any uncommitted modification:** stop and ask before the next call, every time. The user may have repointed the credentials at a different account (prod vs. test) — never assume.

**Effectful calls (POST/PUT/PATCH/DELETE, or any GET known to have side effects such as cursor consumption):** _always_ ask, regardless of `config.yaml` state. Even after consent, you do not run them — see `API-NEVER-MUTATE`.

---

### `API-NEVER-MUTATE` · conduct-only

Never mutate provider state yourself. If verification needs a mutation, author the request and hand it to the user; resume only after they confirm they've run it. Consent can lift this.

---

### `API-ROUTE-THROUGH-BRUNO` · checkable-from-diff

Route every provider call through the connector's `bruno/` collection. Never bypass it with ad-hoc `curl`.

If the pre-request script is broken, fix the script — don't work around it. `curl` skips the sops auth machinery and leaves no saved-example artifact, so the call produces no evidence a later session can use.

Checkable from a diff: a `curl` invocation against the provider's host in any committed script, test, or doc is a finding.

---

### `API-TOKEN-EPHEMERAL` · checkable-from-diff

The decrypted token lives on `req` for the in-flight request only. Never `bru.setVar`, never `bru.setEnvVar`, never write it to a committed file — it is re-derived from `sops` on every request, in both the CLI and the GUI.

This is why the per-request OAuth refresh roundtrip in [auth-wiring.md](auth-wiring.md) is intentional rather than wasteful: caching the access token would persist it into a committed environment file.

Runtime vars _are_ fine for non-secret resource ids chained between requests — the prohibition is specifically on credentials.

---

### `API-DONT-READ-CREDS` · conduct-only

Don't read the encrypted credentials file directly, even just to check its structure. Ask the user where it lives and what JSON path the token sits at.

---

### `API-BUDGET-20RPH` · needs-runtime-evidence

Survey the provider's rate limits before running anything repeatedly, and set the session budget from the result:

- **All required endpoints > 20 req/hr:** run `flowctl raw preview-next`, `flowctl raw discover` (whenever it hits the API — see `API-DISCOVER-STATIC-IS-FREE`), `pytest` (including snapshot tests that drive `flowctl raw preview-next` underneath), and capture-running commands as often as needed, without further consent.
- **Any required endpoint ≤ 20 req/hr:** ask before each run — `flowctl raw discover` included, whenever it hits the API — and apply `API-COST-SAVER-DISABLE`.

Verified limits live in `<connector>/bruno/opencollection.yml`'s `docs:` block under `## API constraints (account-wide)`. Check there before re-deriving anything; record newly-established limits there so the next session doesn't repeat the work.

---

### `API-DISCOVER-STATIC-IS-FREE` · checkable-from-diff

`flowctl raw discover` follows `API-BUDGET-20RPH` **unless** this connector's discovery makes zero provider calls, in which case it's unconditionally free.

Which case you're in is a property of the connector, not a blanket exemption — read its discovery path before assuming either way:

- **Static (the common case):** the resource list is fixed in code. Zero provider calls. Discover is free, full stop.
- **Dynamic:** discovery asks the provider what resources exist. `source-salesforce-native`'s `all_resources` fetches the instance URL and the queryable-object list; `source-zuora`'s fetches its object names. For these, discover is just another API-hitting command.

---

### `API-COST-SAVER-DISABLE` · checkable-from-diff

`disable: true` on a binding in `test.flow.yaml` skips that stream during preview and capture. When the budget is tight, or you only care about one stream's behavior, disable unrelated bindings before running tests.

**Restore them before committing.** A committed `disable: true` on an unrelated binding is a finding — it silently narrows what the test suite covers.
