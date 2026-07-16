# Obfuscation scripts

Utilities for producing sanitized copies of Estuary data and logs — for sharing
reproductions, debugging, or test fixtures without leaking customer content.

| Script | Input | What it obfuscates |
| --- | --- | --- |
| `obfuscate_jsonl.py` | A JSONL collection document file + a Flow catalog | Every field value except the collection key and `_meta` |
| `obfuscate_logs.py` | Connector logs (Flow ops logs / logrus JSON) | Only values connectors explicitly marked as sensitive |
| `obfuscate_common.py` | — | Shared obfuscation core (not run directly) |

## How obfuscation works

All three share one deterministic, **shape-preserving** core (`obfuscate_common.py`):

- **Deterministic** — the same input value plus `--salt` always maps to the same
  output. A value that is unchanged across two records/lines stays unchanged
  after obfuscation, so diffs are preserved. The mapping is value-based (not
  field-based), so cross-references between fields survive too.
- **Shape-preserving** — strings keep their length and per-character class
  (letter→letter of the same case, digit→digit, punctuation/separators kept),
  numbers keep sign and magnitude, and RFC3339 date-times are shifted to another
  *valid* date-time. Booleans are flipped deterministically; `null` stays `null`.
- **Consistent across artifacts** — because both scripts use the same core, a
  given value obfuscates identically whether it appears in a materialized
  document or in a log line.

Change `--salt` to get a different (but still internally consistent) mapping.

## `obfuscate_jsonl.py` — document values

```bash
python3 obfuscate_jsonl.py <input.jsonl> <catalog.flow.yaml> [-c COLLECTION] [-o OUT]
```

Obfuscates every field value **except**, which are passed through unchanged:

- the collection's key fields (the `key:` JSON pointers from the matching
  collection in the catalog),
- `/_meta/uuid`, `/_meta/source`, `/_meta/op`.

These are preserved so the documents stay valid and reducible: the key still
identifies each document and the UUID still drives reduction ordering.

- The catalog may be YAML or JSON. If it defines more than one collection, pass
  `-c/--collection`. JSON catalogs need no dependencies; YAML needs PyYAML — if
  the current interpreter lacks it, the script re-execs into one that has it.
- `-` reads the JSONL from stdin; output defaults to stdout.

## `obfuscate_logs.py` — marked log values

```bash
python3 obfuscate_logs.py <log> [--mode obfuscate|redact|keep-markers] [-o OUT]
```

Connectors do **not** log raw customer data. Where a value genuinely must appear
in a log or error message, the connector wraps it with sentinel code points
(`U+E000` … `U+E001`) via the Go [`go/logsanitize`](../../go/logsanitize)
package. This script finds those marked spans and rewrites the value inside them;
everything else in the line is left untouched. It handles both JSON logs (decoded,
obfuscated, re-encoded so the output stays valid JSON) and plain text.

Modes:

- `obfuscate` (default) — replace with shape-preserving obfuscated text, drop the markers.
- `redact` — replace the value with `«redacted»`, drop the markers.
- `keep-markers` — obfuscate but leave the markers in place, for traceability.

### Age gate — logs from before the feature are refused

The marking only exists in logs emitted **after** connectors shipped the
`logsanitize` change (`MARKING_RELEASED` in the script). In older logs there are
no markers, so the *absence* of a marked span does not mean the *absence* of
sensitive data — obfuscating such a log would produce output that looks safe but
isn't.

To prevent that, the script reads each line's timestamp (`ts`, then `time`,
`timestamp`, `@ts`, `@timestamp`) and **refuses the whole batch** — before writing
any output — if any line predates the release, or if a line has no timestamp it
can verify. The error is explicit:

> these logs are not clearly marked with sensitive information and as such cannot
> be safely obfuscated

Flags:

- `--skip-age-check` — bypass the gate. **Unsafe**; only use it for logs you
  independently know postdate the marking release (e.g. in tests).

> **Maintainers:** set `MARKING_RELEASED` to the actual production release/deploy
> date of the `logsanitize` change.

## Marking new sensitive values

If a connector must log a customer-derived value, wrap it at the source with
`go/logsanitize` (`Value` / `Quoted` / `Goval`) rather than emitting it directly.
`grep -rn logsanitize.` is the canonical inventory of every site that knowingly
logs customer data.
