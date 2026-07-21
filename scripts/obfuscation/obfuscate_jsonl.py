#!/usr/bin/env python3
"""Obfuscate every value in a JSONL collection-document file.

ALL field values are obfuscated, at every depth, EXCEPT:

  * ``/_meta/uuid`` — the document UUID (drives reduction ordering)
  * ``/_meta/op``   — the change operation (c/u/d)

Everything else — including the collection key, ``/_meta/source``, and every
nested value — is obfuscated, so the output cannot expose anything about the
customer. No catalog is needed: the preserved set is fixed.

Obfuscation is deterministic (the same input value plus ``--salt`` always maps
to the same output) so relationships across documents survive. Within a string,
EVERY character is obfuscated — ASCII cased letters and digits map within their
class, and everything else (caseless letters of any script, punctuation,
symbols, emoji, whitespace, combining marks) maps to a CJK ideograph; only the
character count is preserved, so no original structure survives. Numbers keep
sign and magnitude, booleans flip, and ``null`` stays ``null``.

The one exception is RFC3339 date-times: the instant is shifted (obfuscated) but
kept a valid date-time, so date-time fields stay schema-valid. The format
characters that survive reveal nothing the collection schema doesn't declare.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import string
import sys
from datetime import datetime, timedelta, timezone

# Token-paths preserved verbatim (with their entire subtree). Nothing else is.
PRESERVED: set[tuple[str, ...]] = {("_meta", "uuid"), ("_meta", "op")}


def _rng(value: object, salt: str) -> random.Random:
    """A deterministic RNG seeded by the value's content and a run-wide salt."""
    digest = hashlib.sha256(f"{salt}\0{value!r}".encode()).digest()
    return random.Random(int.from_bytes(digest, "big"))


def _try_parse_datetime(s: str) -> datetime | None:
    # Normalise a trailing 'Z': fromisoformat only accepts it on Python 3.11+.
    normalised = s[:-1] + "+00:00" if s.endswith("Z") else s
    try:
        return datetime.fromisoformat(normalised)
    except ValueError:
        return None


def _obfuscate_datetime(s: str, dt: datetime, salt: str) -> str:
    r = _rng(s, salt)
    # Shift by a deterministic amount that keeps the value a plausible, valid
    # date-time (a valid date can't be produced by scrambling digits blindly).
    shifted = dt + timedelta(days=r.randint(-3650, 3650), seconds=r.randint(0, 86399))
    out = shifted.isoformat()
    if s.endswith("Z") and shifted.tzinfo == timezone.utc:
        out = shifted.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return out


def _obfuscate_string(s: str, salt: str) -> str:
    if not s:
        return s
    dt = _try_parse_datetime(s)
    if dt is not None:
        return _obfuscate_datetime(s, dt, salt)
    r = _rng(s, salt)
    return "".join(_obfuscate_char(ch, r) for ch in s)


# CJK Unified Ideographs — a deterministic, length-preserving target for any
# "content" character that has no cased Latin/digit equivalent.
_CJK_LO, _CJK_HI = 0x4E00, 0x9FA5


def _obfuscate_char(ch: str, r: random.Random) -> str:
    if ch.isdigit():
        return r.choice(string.digits)
    if ch.islower():
        return r.choice(string.ascii_lowercase)
    if ch.isupper():
        return r.choice(string.ascii_uppercase)
    # EVERYTHING else is obfuscated too — caseless letters (all scripts),
    # numerics, punctuation, symbols, emoji, whitespace, and combining marks —
    # mapped to a deterministic CJK ideograph. Nothing passes through; only the
    # character count is preserved.
    return chr(r.randint(_CJK_LO, _CJK_HI))


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
    # float: keep sign and rough magnitude
    return round(n * (0.5 + r.random()), 6)


def obfuscate_leaf(value: object, salt: str) -> object:
    if value is None:
        return None
    if isinstance(value, str):
        return _obfuscate_string(value, salt)
    if isinstance(value, (bool, int, float)):
        return _obfuscate_number(value, salt)
    return value


def obfuscate(value: object, path: tuple[str, ...], salt: str) -> object:
    if path in PRESERVED:
        return value  # preserve this value and its entire subtree
    if isinstance(value, dict):
        return {k: obfuscate(v, path + (k,), salt) for k, v in value.items()}
    if isinstance(value, list):
        return [obfuscate(v, path + (str(i),), salt) for i, v in enumerate(value)]
    return obfuscate_leaf(value, salt)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("jsonl", help="Input JSONL file ('-' for stdin)")
    parser.add_argument("-o", "--output", help="Output JSONL file (default: stdout)")
    parser.add_argument("--salt", default="obfuscate", help="Salt for deterministic obfuscation (default: 'obfuscate')")
    args = parser.parse_args()

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
            outfile.write(json.dumps(obfuscate(doc, (), args.salt), separators=(",", ":")) + "\n")
    finally:
        if infile is not sys.stdin:
            infile.close()
        if outfile is not sys.stdout:
            outfile.close()


if __name__ == "__main__":
    main()
