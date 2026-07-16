"""Shared, shape-preserving obfuscation core.

Used by both obfuscate_jsonl.py (obfuscates document field values) and
obfuscate_logs.py (obfuscates values marked with logsanitize sentinels in
connector logs). Keeping the core here means a given value obfuscates
identically in either context, so diffs stay consistent across artifacts.

Obfuscation is deterministic (same input + salt -> same output) and preserves
the shape of values: string length and per-character class, number sign and
magnitude, and RFC3339 date-times stay valid date-times.
"""

from __future__ import annotations

import hashlib
import random
import string
from datetime import datetime, timedelta, timezone


def rng(value: object, salt: str) -> random.Random:
    """A deterministic RNG seeded by the value's content and a run-wide salt."""
    digest = hashlib.sha256(f"{salt}\0{value!r}".encode()).digest()
    return random.Random(int.from_bytes(digest, "big"))


def _try_parse_datetime(s: str) -> datetime | None:
    # Normalise a trailing 'Z': fromisoformat only accepts it on Python 3.11+,
    # and this may run under an older interpreter.
    normalised = s[:-1] + "+00:00" if s.endswith("Z") else s
    try:
        return datetime.fromisoformat(normalised)
    except ValueError:
        return None


def _obfuscate_datetime(s: str, dt: datetime, salt: str) -> str:
    r = rng(s, salt)
    # Shift by a deterministic amount that keeps the value a plausible, valid
    # date-time (a valid date can't be produced by scrambling digits blindly).
    shifted = dt + timedelta(days=r.randint(-3650, 3650), seconds=r.randint(0, 86399))
    out = shifted.isoformat()
    if s.endswith("Z") and shifted.tzinfo == timezone.utc:
        out = shifted.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return out


def obfuscate_string(s: str, salt: str) -> str:
    if not s:
        return s
    dt = _try_parse_datetime(s)
    if dt is not None:
        return _obfuscate_datetime(s, dt, salt)
    r = rng(s, salt)
    chars = []
    for ch in s:
        if ch.islower():
            chars.append(r.choice(string.ascii_lowercase))
        elif ch.isupper():
            chars.append(r.choice(string.ascii_uppercase))
        elif ch.isdigit():
            chars.append(r.choice(string.digits))
        else:
            chars.append(ch)  # punctuation / whitespace / separators kept
    return "".join(chars)


def obfuscate_number(n: int | float, salt: str) -> int | float:
    r = rng(n, salt)
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
        return obfuscate_string(value, salt)
    if isinstance(value, (bool, int, float)):
        return obfuscate_number(value, salt)
    return value
