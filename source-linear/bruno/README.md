# Where this collection lives, and why it isn't in `source-linear/` yet

This collection is **not** at `source-linear/bruno/` on purpose.

The stream-builder session that authored it runs as a background job and is not allowed to
write into the shared git checkout — parallel sessions would collide, each dropping its own
`bruno/` into the same path. Its worktree-isolation tool was unavailable, so the collection
was authored here instead. **The orchestrator lands it at `source-linear/bruno/` during
serial integration**, which is the intended end state; `environments/Linear.bru` already
carries the connector-relative `config_path: ../config.yaml` that will be correct there.

Until then, run it from here with an absolute `config_path` override.

## Running it

```bash
cd /Users/jonwihl/.claude/jobs/3325b132/tmp/bruno

BRU=/Users/jonwihl/.claude/jobs/3325b132/tmp/tools/node_modules/.bin/bru
CFG=/Users/jonwihl/Desktop/ConnectorSkills/connectors/source-linear/config.yaml

# all 19 read-only requests
"$BRU" run . --env Linear --sandbox developer --env-var config_path=$CFG

# a single request
"$BRU" run "14 - Issues Include Archived.bru" --env Linear --sandbox developer --env-var config_path=$CFG
```

`bru` was not installed on this machine; a local copy is vendored at `../tools/`. For a
permanent install: `npm i -g @usebruno/cli`. (An earlier copy under `/tmp` was wiped by
system cleanup — don't rely on `/tmp` for this.)

Once the collection is landed under the connector, drop the `--env-var` override entirely
and run it from `source-linear/bruno/`.

## Do NOT add `-r` / `--recursive`

`bru run .` is non-recursive, and that is the only reason it skips `Seeding/`. With `-r` it
would execute all four **mutations** (create project, create initiative, edit an issue,
archive an issue) in one shot. Run seeding requests individually and deliberately.

## Layout

| path | contents |
|---|---|
| `collection.bru` | sops pre-request auth, rate-limit header echo, and the `## API constraints (account-wide)` docs block |
| `environments/Linear.bru` | `base_url`, `config_path` |
| `01`–`07` | shared probes (auth, introspection, ordering direction, complexity, page-size ceiling, cursor boundary) |
| `10`–`14` | Issues |
| `20`–`21` | Projects |
| `30`–`31` | Initiatives |
| `40`–`42` | Labels |
| `Seeding/` | **mutations — user-run only.** `A1`,`A2` independent; run `D1` then `D2`. |

Every read-only request carries a saved `example { }` block with the observed status,
rate-limit/complexity headers, and body. See `../bruno-manifest.md` for the full inventory
and what each request proves, and `../plan.md` for the implementation plan.

## Auth

`collection.bru`'s `script:pre-request` shells `sops -d --output-type=json` and attaches
`credentials.access_token_sops` as a **bare** `Authorization` header — no `Bearer` prefix —
mirroring `source_linear/resources.py:11-29`. The decrypted token exists only on `req` for
the in-flight request; it is never written to an environment file or any other on-disk
artifact.

Requires `--sandbox developer` (the script uses Node's `child_process`). In the Bruno GUI,
set the collection's JS sandbox to **developer** mode.
