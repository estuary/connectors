# source-linear Bruno collection

Read-only probes that established the Linear API behaviour this connector relies on, plus
four user-run seeding mutations. `MANIFEST.md` lists what each request proves.

## Running it

Install the CLI once with `npm i -g @usebruno/cli`, then from this directory:

```bash
# all 21 read-only requests
bru run . --env Linear --sandbox developer

# a single request
bru run "14 - Issues Include Archived.bru" --env Linear --sandbox developer
```

`environments/Linear.bru` sets `config_path: ../config.yaml`, the connector's sops-encrypted
test config, so no override is needed.

## Do NOT add `-r` / `--recursive`

`bru run .` is non-recursive, and that is the only reason it skips `Seeding/`. With `-r` it
would execute all four **mutations** (create project, create initiative, edit an issue,
archive an issue) in one shot. Run seeding requests individually and deliberately.

## Layout

| path | contents |
|---|---|
| `collection.bru` | sops pre-request auth, rate-limit header echo, and the `## API constraints (account-wide)` docs block |
| `environments/Linear.bru` | `base_url`, `config_path` |
| `01`–`08` | shared probes (auth, introspection, ordering direction, complexity, page-size ceiling, cursor boundary, archival capability) |
| `10`–`15` | Issues |
| `20`–`21` | Projects |
| `30`–`31` | Initiatives |
| `40`–`42` | Labels |
| `Seeding/` | **mutations — user-run only.** `A1`,`A2` independent; run `D1` then `D2`. |

Every read-only request carries a saved `example { }` block with the observed status,
rate-limit/complexity headers, and body.

## Auth

`collection.bru`'s `script:pre-request` shells `sops -d --output-type=json` and attaches
`credentials.access_token_sops` as a **bare** `Authorization` header — no `Bearer` prefix —
mirroring `AUTHORIZATION_HEADER` and `_token_source` in `source_linear/resources.py`. The
decrypted token exists only on `req` for the in-flight request; it is never written to an
environment file or any other on-disk artifact.

Requires `--sandbox developer` (the script uses Node's `child_process`). In the Bruno GUI,
set the collection's JS sandbox to **developer** mode.
