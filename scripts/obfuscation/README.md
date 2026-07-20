# Obfuscation scripts

Utilities for producing sanitized copies of Estuary catalogs and collection data —
for sharing reproductions, debugging, or test fixtures without exposing anything
about the customer.

| Script | Input | What it does |
| --- | --- | --- |
| `obfuscate_jsonl.py` | A JSONL collection-document file | Obfuscates **every** value except `/_meta/uuid` and `/_meta/op` |
| `strip_catalog.py` | A Flow catalog (YAML/JSON) | Strips connector configs to an **allow-list** of customer-agnostic knobs |
| `config_allowlist.txt` | — | The allow-list of config keys `strip_catalog.py` keeps |

## `obfuscate_jsonl.py` — collection documents

```bash
python3 obfuscate_jsonl.py <input.jsonl> [-o OUT] [--salt SALT]
```

Obfuscates **absolutely everything** — every field value at every depth,
including the collection key and `/_meta/source` — **except** `/_meta/uuid` and
`/_meta/op`, which are preserved (they drive reduction ordering and carry no
customer data). No catalog is needed; the preserved set is fixed.

Obfuscation is **deterministic and shape-preserving**:

- Same input value + `--salt` → same output, so relationships across documents
  survive (the mapping is value-based, not field-based). Change `--salt` for a
  different but still internally-consistent mapping.
- Strings keep length and per-character class (letter→letter same case,
  digit→digit, punctuation/separators kept); numbers keep sign and magnitude;
  RFC3339 date-times shift to another *valid* date-time; booleans flip; `null`
  stays `null`.

`-` reads from stdin; output defaults to stdout.

## `strip_catalog.py` — catalog config

```bash
python3 strip_catalog.py <catalog.flow.yaml> [-o OUT]
```

Connector endpoint configs and binding resource configs are full of
customer-identifying data (hosts, credentials, account/user names, database /
schema / table names, buckets, regions, …). This tool removes **all** of it and
keeps **only** keys on the allow-list in `config_allowlist.txt`.

It is an **allow-list, strip-by-default** design: a config value survives only if
its own leaf key name is allow-listed — recursively, so an agnostic knob nested
inside an otherwise-stripped block (e.g. `advanced.batch_size`) is kept while its
customer-specific siblings (e.g. `advanced.skip_backfills`) are dropped. Anything
unknown is removed, so the output is **guaranteed not to expose customer
information** even as connectors add new, unclassified fields. The trade-off is
that a genuinely-agnostic new knob is stripped until added to the allow-list —
safe, never a leak.

It processes every connector/`local` `config` and every binding `resource`
across captures, materializations, and connector-based derivations. Structural
catalog fields (image, collection schemas, keys, targets/sources) are left
intact. YAML in → YAML out; JSON in → JSON out. (YAML needs PyYAML; if the
current interpreter lacks it, the script re-execs into one that has it, and JSON
catalogs work with no dependency.)

### Maintaining the allow-list

`config_allowlist.txt` is one key per line (`#` comments allowed). Add a key
**only** if it reveals nothing about the customer — a pure behaviour / format /
performance / scheduling toggle. When in doubt, leave it out. Both camelCase and
snake_case spellings are listed because Go and Python connectors differ.
