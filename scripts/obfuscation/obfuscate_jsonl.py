#!/usr/bin/env python3
"""Obfuscate every value in a JSONL collection-document file.

ALL field values are obfuscated, at every depth, EXCEPT ``/_meta/uuid`` and
``/_meta/op`` (the document UUID and change operation, which carry no customer
data and drive reduction). The collection key, ``/_meta/source``, and every
nested value are obfuscated.

Field names: a document's object keys are preserved only when they are part of
the collection schema (pass ``--schema`` or ``--catalog``/``--collection``).
Any key present in a document but NOT defined by the schema — i.e. a
customer-controlled / dynamic key, as produced by document stores and
``additionalProperties`` schemas — is obfuscated too. With no schema, every key
is treated as unknown and obfuscated (``_meta`` and its fields are always kept
as Flow structure).

Obfuscation is deterministic (the same input value always maps to the same
output) so relationships across documents survive. Within a string, every
character is obfuscated — ASCII cased letters and digits map within their class,
everything else (caseless letters of any script, punctuation, symbols, emoji,
whitespace, combining marks) maps to a CJK ideograph; only the character count
is preserved. Numbers keep sign and magnitude, booleans flip, ``null`` stays.

RFC3339 date-times are replaced with a random valid date-time (or date) in UTC,
so date-time fields stay schema-valid while leaking neither the instant, the
original timezone, nor sub-second precision.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import string
import sys
from datetime import datetime, timedelta, timezone

from _load import load_structured
from _salt import user_salt

# Token-paths preserved verbatim (with their entire subtree). Nothing else is.
PRESERVED: set[tuple[str, ...]] = {("_meta", "uuid"), ("_meta", "op")}


def _rng(value: object, salt: str) -> random.Random:
    """A deterministic RNG seeded by the value's content and the user's salt."""
    digest = hashlib.sha256(f"{salt}\0{value!r}".encode()).digest()
    return random.Random(int.from_bytes(digest, "big"))


def _looks_like_datetime(s: str) -> datetime | None:
    normalised = s[:-1] + "+00:00" if s.endswith("Z") else s
    try:
        return datetime.fromisoformat(normalised)
    except ValueError:
        return None


# ~1970..2036, in seconds, for the randomized replacement instant.
_MAX_EPOCH = 2_082_758_399


def _obfuscate_datetime(s: str, salt: str) -> str:
    r = _rng(s, salt)
    shifted = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=r.randint(0, _MAX_EPOCH))
    if ":" not in s:  # a date with no time component
        return shifted.date().isoformat()
    return shifted.strftime("%Y-%m-%dT%H:%M:%SZ")


# CJK Unified Ideographs — a deterministic, length-preserving target for any
# character that has no cased Latin/digit equivalent.
_CJK_LO, _CJK_HI = 0x4E00, 0x9FA5


def _obfuscate_char(ch: str, r: random.Random) -> str:
    if ch.isdigit():
        return r.choice(string.digits)
    if ch.islower():
        return r.choice(string.ascii_lowercase)
    if ch.isupper():
        return r.choice(string.ascii_uppercase)
    # Everything else — caseless letters (all scripts), numerics, punctuation,
    # symbols, emoji, whitespace, combining marks — maps to a CJK ideograph.
    return chr(r.randint(_CJK_LO, _CJK_HI))


def _obfuscate_string(s: str, salt: str) -> str:
    if not s:
        return s
    if _looks_like_datetime(s) is not None:
        return _obfuscate_datetime(s, salt)
    r = _rng(s, salt)
    return "".join(_obfuscate_char(ch, r) for ch in s)


def _obfuscate_number(n: int | float, salt: str) -> int | float:
    r = _rng(n, salt)
    if isinstance(n, bool):  # bool is a subclass of int — handle before int
        return r.random() < 0.5
    if isinstance(n, int):
        if n == 0:
            return r.randint(0, 9)
        digits = len(str(abs(n)))
        magnitude = r.randint(10 ** (digits - 1), 10 ** digits - 1)
        return magnitude if n > 0 else -magnitude
    return round(n * (0.5 + r.random()), 6)  # float: keep sign and rough magnitude


def obfuscate_leaf(value: object, salt: str) -> object:
    if value is None:
        return None
    if isinstance(value, str):
        return _obfuscate_string(value, salt)
    if isinstance(value, (bool, int, float)):
        return _obfuscate_number(value, salt)
    return value


def obfuscate(value: object, path: tuple[str, ...], known: set[str], salt: str,
              *, keep_ctx: bool = True, in_meta: bool = False) -> object:
    if path in PRESERVED:
        return value  # preserve this value and its entire subtree
    if isinstance(value, dict):
        out: dict[object, object] = {}
        for k, v in value.items():
            child = path + (k,)
            structural = in_meta or (path == () and k == "_meta")
            if structural:
                out_key, child_keep, child_meta = k, True, True
            elif keep_ctx and k in known:
                out_key, child_keep, child_meta = k, True, False
            else:
                out_key, child_keep, child_meta = _obfuscate_string(k, salt), False, False
            out[out_key] = obfuscate(v, child, known, salt, keep_ctx=child_keep, in_meta=child_meta)
        return out
    if isinstance(value, list):
        return [obfuscate(v, path + (str(i),), known, salt, keep_ctx=keep_ctx, in_meta=in_meta)
                for i, v in enumerate(value)]
    return obfuscate_leaf(value, salt)


def collect_known_fields(schema: object) -> set[str]:
    """Every property name defined anywhere in a JSON schema. A document key is
    kept only if it is one of these; anything else is obfuscated."""
    names: set[str] = set()

    def walk(node: object) -> None:
        if isinstance(node, dict):
            props = node.get("properties")
            if isinstance(props, dict):
                names.update(str(k) for k in props)
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    walk(schema)
    return names


def known_fields_from_args(args: argparse.Namespace) -> set[str]:
    if args.schema:
        schema, _ = load_structured(args.schema)
        return collect_known_fields(schema)
    if args.catalog:
        catalog, _ = load_structured(args.catalog)
        collections = (catalog or {}).get("collections") if isinstance(catalog, dict) else None
        if not collections:
            raise SystemExit(f"no collections in catalog {args.catalog!r}")
        name = args.collection
        if name is None:
            if len(collections) == 1:
                name = next(iter(collections))
            else:
                raise SystemExit("catalog has multiple collections; pass --collection")
        elif name not in collections:
            raise SystemExit(f"collection {name!r} not in catalog")
        spec = collections[name]
        known: set[str] = set()
        for key in ("schema", "writeSchema", "readSchema"):
            if isinstance(spec.get(key), dict):
                known |= collect_known_fields(spec[key])
        return known
    return set()  # no schema -> every key is unknown (fail-safe)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("jsonl", help="Input JSONL file ('-' for stdin)")
    parser.add_argument("-o", "--output", help="Output JSONL file (default: stdout)")
    parser.add_argument("--schema", help="JSON-Schema file (YAML/JSON) defining known field names")
    parser.add_argument("--catalog", help="Flow catalog (YAML/JSON) to take the collection schema from")
    parser.add_argument("--collection", help="Collection name in --catalog (required if it has more than one)")
    args = parser.parse_args()

    known = known_fields_from_args(args)
    salt = user_salt()

    infile = sys.stdin if args.jsonl == "-" else open(args.jsonl)
    outfile = open(args.output, "w") if args.output else sys.stdout
    try:
        for lineno, line in enumerate(infile, 1):
            line = line.strip()
            if not line:
                continue
            try:
                doc = json.loads(line)
            except json.JSONDecodeError as e:
                raise SystemExit(f"{args.jsonl}:{lineno}: invalid JSON: {e}")
            outfile.write(json.dumps(obfuscate(doc, (), known, salt), separators=(",", ":")) + "\n")
    finally:
        if infile is not sys.stdin:
            infile.close()
        if outfile is not sys.stdout:
            outfile.close()


if __name__ == "__main__":
    main()
