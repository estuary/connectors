# Bruno manifest — source-linear (Issues / Projects / Initiatives / Labels)

Collection: `/Users/jonwihl/.claude/jobs/3325b132/tmp/bruno/`
→ **orchestrator lands it at `source-linear/bruno/` during integration.**

It is deliberately NOT in the shared checkout: background sessions can't write there
(parallel stream-builders would collide on the same `bruno/` path) and worktree isolation
was unavailable. `environments/Linear.bru` already holds the connector-relative
`config_path: ../config.yaml`, correct for its final home. See `bruno/README.md` for
run instructions from the current location.

`bru` is vendored at `tmp/tools/node_modules/.bin/bru` (not installed on this machine; an
earlier `/tmp` copy was wiped by system cleanup). Permanent install: `npm i -g @usebruno/cli`.

- **21** read-only requests (GraphQL `query` documents), 21/21 green. Executed live in two
  rounds against org `estuary-test`: **2026-07-30** (pre-seed) and **2026-08-04**
  (post-seed). Every one carries a saved `example { }` block — all regenerated against the
  post-seed workspace — with the observed status, rate-limit/complexity headers, and body.
- 5 mutating requests in `Seeding/` — **authored only, never run by me.**

## Running it

```bash
cd source-linear/bruno
bru run . --env Linear --sandbox developer     # 21 read-only requests
```

> **Do not add `-r` / `--recursive` to that command.** `bru run .` is non-recursive, which
> is the only reason it skips `Seeding/`. Adding `-r` would execute all five MUTATIONS
> (create project, create initiative, edit an issue, archive an issue) in one go. Run
> seeding requests individually and deliberately.

`environments/Linear.bru` sets `config_path: ../config.yaml`, correct once the collection
lives inside the connector. From anywhere else, override:
`--env-var config_path=/abs/path/to/source-linear/config.yaml`.

Auth: `collection.bru`'s `script:pre-request` shells `sops -d --output-type=json` and
attaches `credentials.access_token_sops` as a **bare** `Authorization` header (no `Bearer`),
mirroring `source_linear/resources.py:11-29`. The token lives on `req` only — never
`bru.setVar`, never written to disk.

`collection.bru`'s `script:post-response` echoes `X-Complexity` and the rate-limit headers
on every run. No body redaction is configured (no HATEOAS envelope, no volatile ids; the
fields that churn are the ones the probes exist to observe).

## Read-only requests

Bands: 1–9 shared probes, 10–19 Issues, 20–29 Projects, 30–39 Initiatives, 40–49 Labels.

| # | Request | Proves | Saved example |
|---|---|---|---|
| 01 | Viewer Probe | auth wiring; **live limits are 2500 req/hr**, not 5000 | `200 — auth OK + live rate-limit ceilings (2500 req/hr)` |
| 02 | Introspect Pagination Args | all 4 root fields share the arg set; `sort` on 3 of 4; enum descriptions null | `200 — projection: PaginationOrderBy + cluster root-field args` |
| 03 | Introspect Entity Types | `archivedAt` on all 4; `updatedAt` NON_NULL; `IssueLabel.team` nullable | `200 — projection: cursor/archival fields per entity type` |
| 04 | OrderBy Direction Probe | **`orderBy` is DESCENDING** (via `users`, the only distinct-timestamp collection) | `200 — DESCENDING confirmed (newest first)` |
| 05 | Complexity Envelope | 10k cap is enforced on **actual** cost, not the documented static estimate | `200 — 250x250 nesting NOT rejected; issues cap is actual-cost` |
| 06 | Page Size Over Limit | **max `first` = 250**, rejected not clamped — and served as **HTTP 200** | `200 + errors — first=251 rejected, not clamped (data: null)` |
| 07 | Cursor Filter Boundary | `gt` exclusive / `gte` inclusive at 1 ms; the tie hazard, live | `200 — gt exclusive / gte inclusive at 1ms boundary` |
| 08 | Archival Capability Matrix | **`archivedAt` filterable on `IssueFilter` ONLY**; `IssueSortInput` has no `archivedAt`; records the accepted limitation | `200 — archivedAt filterable on IssueFilter ONLY` |
| 10 | Issues Bare | production selection set; full representative item | `200 — full representative Issue (2 of 4)` |
| 11 | Issues Ascending Sweep | **ascending `updatedAt` CONFIRMED** post-seed; `sort` cursor is a base64 keyset | `200 — ASCENDING updatedAt CONFIRMED (newest last, post-seed)` |
| 12 | Issues Pagination Walk | page 1; `endCursor` is the raw node UUID under `orderBy` | `200 — page 1 of 2; endCursor is the raw node UUID` |
| 13 | Issues Pagination Walk Page 2 | walk reassembles exactly; `hasNextPage:false` is the completion signal | `200 — page 2, hasNextPage=false, walk reassembles` |
| 14 | Issues Include Archived | **VERIFIED NEGATIVE — archiving does NOT bump `updatedAt`**; withArchived=4 / liveOnly=3 | `200 — withArchived=4 / liveOnly=3; updatedAt NOT bumped by archive` |
| 15 | Issues Archival Sweep | archival pass finds EST-2 (stale `updatedAt`); `includeArchived` still mandatory | `200 — archivedAt filter finds EST-2; includeArchived still mandatory` |
| 20 | Projects Bare | populated; **caught entitlement-gated `identifier`** (now removed) | `200 — populated Project (identifier removed: entitlement-gated)` |
| 21 | Projects Incremental Sweep | `ProjectFilter.updatedAt` + `sort` accepted; 1 row post-seed | `200 — updatedAt filter + sort accepted, 1 row` |
| 30 | Initiatives Bare | populated; `status` is a plain STRING (vs Project's object); caught `priorityLabel` + `identifier` | `200 — populated Initiative (status is a STRING, not an object)` |
| 31 | Initiatives Incremental Sweep | `InitiativeFilter.updatedAt` + `sort` accepted; 1 row post-seed | `200 — updatedAt filter + sort accepted, 1 row` |
| 40 | Labels Bare | production selection set; workspace-vs-team scoping via nullable `team` | `200 — full representative IssueLabel (3 workspace-level labels)` |
| 41 | Labels Incremental Sweep | filter narrows **3 → 0** across the 1 ms boundary | `200 — updatedAt filter narrows 3 -> 0 at exact boundary` |
| 42 | Labels Sort Rejected | `issueLabels` has no `sort` — **HTTP 400** `GRAPHQL_VALIDATION_FAILED` | `400 — GRAPHQL_VALIDATION_FAILED: unknown argument "sort"` |

Chaining: `12` captures `endCursor` into the runtime var `issues_page1_cursor` via
`script:post-response`; `13` consumes it, with a conditional pre-request fallback for
out-of-order runs.

Bodies follow proportionality — `Bare` requests keep one full representative item; probes
(`02`, `03`, `07`, `08`, `14`, `15`, `21`, `31`, `41`) save a projection of just the decision-relevant fields, noted
in the example name.

## Seeding requests — COMPLETE (run 2026-08-04 by the user/orchestrator)

All 5 are GraphQL `mutation` documents with the verb visible. **I did not run any of them.**

| # | Request | Closed | Result |
|---|---|---|---|
| A1 | Create Project | Projects never observed with data | populated; exposed the entitlement-gated `identifier` bug |
| A2 | Create Initiative | Initiatives body + entitlement check | populated; **entitlement present** |
| D1 | Touch Issue Updated At | ascending order; partial narrowing | EST-1 `updatedAt` -> `2026-08-04T18:12:51.985Z`; ascending confirmed |
| D2 | Archive Issue | **does archiving bump `updatedAt`?** | **NO — verified negative.** EST-2 `archivedAt` set, `updatedAt` unchanged |

Residual disposable state in `estuary-test`: project `1904e338…`, initiative `44f4e104…`,
archived issue EST-2, EST-1's bumped `updatedAt`. **Recommend leaving it** — it keeps the
discover snapshots non-empty for Projects/Initiatives and preserves the only archived row
available to exercise the Issues archival pass.

## Why the workspace needed seeding (pre-seed state, 2026-07-30 — historical)

| collection | rows | note |
|---|---|---|
| issues | 4 | all share `updatedAt` `2026-07-30T21:35:03.808Z` |
| issueLabels | 3 | same timestamp; all workspace-level |
| projects | 0 | never observed with data (now 1) |
| initiatives | 0 | never observed with data; paid feature, entitlement since confirmed (now 1) |
| archived (any type) | 0 | — (now 1: EST-2) |

Stock Linear onboarding content, bulk-seeded in one transaction — which is why every
timestamp ties and why direction had to be established via `users`.

## Reviewed outcome

Plan status is **FINAL** (`plan.md`). Four gate decisions settled; the archival design was
rewritten after `Seeding/D2` returned a verified negative.

The one accepted limitation, which belongs in the connector's user-facing docs: **archival
and deletion of Projects, Initiatives and Labels are not captured** — no `archivedAt` filter
exists on their filter types and archiving does not advance `updatedAt`. Issues *is* covered,
via the separate `archivedAt` pass in `15 - Issues Archival Sweep`.
