#!/usr/bin/env python3
"""Sanitize the output of `flowctl catalog history --models`.

`flowctl catalog history --name <task> --models --output json` emits one JSON
object (a history row) per line:

    {"catalog_name": "...", "catalog_type": "capture",
     "publication": {"model": {...spec...}, "detail": "...", "publicationId": ...,
                     "publishedAt": "...", "userEmail": "...", "userFullName": "...",
                     "userId": "...", "userAvatarUrl": "..."}}

This tool rewrites each row so the history can be shared without exposing the
customer: every published `model` is sanitized exactly like a catalog spec
(configs stripped to the allow-list, schema annotations and derivation code
removed, names/references obfuscated), the task name is obfuscated, and the
publisher's identity and the free-text detail are obfuscated. The publication
id, timestamp, and catalog type are kept so the timeline stays intact.

Input is read as newline-delimited JSON (flowctl's `--output json`), or a single
JSON array; output is newline-delimited JSON.
"""

from __future__ import annotations

import argparse
import json
import sys

from _salt import user_salt
from obfuscate_jsonl import _obfuscate_string
from strip_catalog import _obf_name, load_allowlist, sanitize_spec

# Publisher identity + free-text fields on a publication (camelCase from the
# GraphQL API, snake_case just in case), obfuscated as values.
_IDENTITY_FIELDS = (
    "detail",
    "userEmail", "user_email",
    "userFullName", "user_full_name",
    "userAvatarUrl", "user_avatar_url",
    "userId", "user_id",
)


def sanitize_row(row: object, allow: set[str], salt: str) -> object:
    if not isinstance(row, dict):
        return row
    for name_field in ("catalog_name", "catalogName"):
        if isinstance(row.get(name_field), str):
            row[name_field] = _obf_name(row[name_field], salt)
    pub = row.get("publication")
    if isinstance(pub, dict):
        if isinstance(pub.get("model"), dict):
            sanitize_spec(pub["model"], allow, salt)
        for field in _IDENTITY_FIELDS:
            if isinstance(pub.get(field), str):
                pub[field] = _obfuscate_string(pub[field], salt)
    return row


def _rows(text: str) -> list:
    text = text.strip()
    if not text:
        return []
    try:
        data = json.loads(text)
        return data if isinstance(data, list) else [data]
    except json.JSONDecodeError:
        return [json.loads(line) for line in text.splitlines() if line.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("history", help="`flowctl catalog history` JSON output ('-' for stdin)")
    parser.add_argument("-o", "--output", help="Output file (default: stdout)")
    args = parser.parse_args()

    allow = load_allowlist()
    salt = user_salt()
    text = sys.stdin.read() if args.history == "-" else open(args.history, encoding="utf-8").read()

    out = open(args.output, "w", encoding="utf-8") if args.output else sys.stdout
    try:
        for row in _rows(text):
            out.write(json.dumps(sanitize_row(row, allow, salt), separators=(",", ":")) + "\n")
    finally:
        if out is not sys.stdout:
            out.close()


if __name__ == "__main__":
    main()
