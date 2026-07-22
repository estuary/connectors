# Obfuscation scripts

Utilities for producing sanitized copies of Estuary catalogs and collection data —
for sharing reproductions, debugging, or test fixtures without exposing anything
about the customer.

| Script | Input | What it does |
| --- | --- | --- |
| `obfuscate_jsonl.py` | A JSONL collection-document file | Obfuscates every value, and every field name not defined by the schema |
| `strip_catalog.py` | A Flow catalog (YAML/JSON) | Strips configs to an allow-list, and obfuscates names / schemas / derivations |
| `config_allowlist.txt` | — | The allow-list of config keys `strip_catalog.py` keeps |

## `obfuscate_jsonl.py` — collection documents

```bash
python3 obfuscate_jsonl.py <input.jsonl> [--schema SCHEMA | --catalog CAT [--collection NAME]] [-o OUT]
```

Obfuscates **every field value** at every depth — including the collection key
and `/_meta/source` — **except** `/_meta/uuid` and `/_meta/op`, which drive
reduction and carry no customer data.

**Field names are scoped to the schema.** Pass the collection schema (via
`--schema`, or `--catalog`/`--collection` to take it from a Flow catalog) and any
object key **defined by the schema** is kept, while any key present in a document
but **not** in the schema — a customer-controlled / dynamic key, as produced by
document stores and `additionalProperties` schemas — is obfuscated like a value.
With no schema, every key is treated as unknown and obfuscated. Keys under
`_meta` are always kept as Flow structure. Once a key is unknown, its whole
subtree is obfuscated (a known field name nested under a dynamic key does not
rescue it).

Obfuscation is **deterministic** — the same input value always maps to the same
output, so relationships across documents survive (the mapping is value-based,
not field-based):

- **Every character in a string is obfuscated.** ASCII cased letters and digits
  map within their class (letter→letter same case, digit→digit); everything else
  — caseless letters of any script (CJK, Japanese, Korean, Arabic, Hebrew, Thai,
  …), punctuation, symbols, emoji, whitespace, and combining marks — maps to a
  CJK ideograph. Only the character count is preserved, so no original structure
  survives. Numbers keep sign and magnitude; booleans flip; `null` stays `null`.
- **RFC3339 date-times** are replaced with a random valid date-time (or date) in
  UTC, so date-time fields stay schema-valid while leaking neither the instant,
  the original timezone, nor sub-second precision.

`-` reads from stdin; output defaults to stdout.

> Residual: string length, number sign/magnitude, array length, and which
> optional fields are present are preserved. If that metadata matters for your
> threat model, don't share these documents.

## `strip_catalog.py` — catalog specs

```bash
python3 strip_catalog.py <catalog.flow.yaml> [-o OUT]
```

Removes or obfuscates every customer-identifying part of a catalog:

- **Connector configs & binding resources** are stripped to an **allow-list,
  strip-by-default**: a config value survives only if its own leaf key name is in
  `config_allowlist.txt` — recursively, so an agnostic knob nested in an
  otherwise-stripped block (e.g. `advanced.sslmode`) is kept while identifying
  siblings (e.g. `advanced.skip_backfills`) are dropped. Anything unknown is
  removed, so the output can't expose customer info even as connectors add new,
  unclassified fields. The trade-off is that a genuinely-agnostic new knob is
  stripped until added to the allow-list — safe, never a leak. `local.command`
  is removed too.
- **Task / collection / materialization names** (including the tenant prefix) and
  every reference to them (`target`, `source`, …) are obfuscated
  segment-by-segment, so internal references stay consistent but no real name
  survives. **Field names in schemas are kept** (they match the documents from
  `obfuscate_jsonl.py`), but schema annotations that can embed literal data —
  `title`, `description`, `$comment`, `examples`, `default`, `const`, `enum` —
  are removed.
- **Derivation code** — transform lambdas, SQL migrations, TypeScript modules —
  is removed, and transform names are obfuscated.

YAML in → YAML out; JSON in → JSON out. (YAML needs PyYAML; if the interpreter
lacks it, the script re-execs into one that has it; JSON works dependency-free.)

### Maintaining the allow-list

`config_allowlist.txt` is one key per line (`#` comments allowed). Only scalar
leaves (and lists of scalars) are kept, matched by their own key name; to keep a
nested object's contents, add each scalar key inside it. Add a key **only** if it
reveals nothing about the customer — a pure behaviour / format / performance /
scheduling toggle. When in doubt, leave it out. Both camelCase and snake_case
spellings are listed because Go and Python connectors differ.
