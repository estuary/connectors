#!/usr/bin/env python3
"""Obfuscate customer values that connectors intentionally emit into logs.

Connectors mark such values with sentinel code points via the Go `logsanitize`
package (U+E000 ... U+E001). This script finds those marked spans in a log
stream and rewrites the value inside them.

It works on both JSON-formatted logs (logrus) and plain text: the markers are
literal code points in either form, and the replacement never introduces
JSON-significant characters, so JSON lines stay valid.

Modes:
  obfuscate     (default) replace with shape-preserving obfuscated text, drop markers
  redact        replace the value with «redacted», drop markers
  keep-markers  obfuscate but leave the markers in place, for traceability

Obfuscation is deterministic and shares its core with obfuscate_jsonl.py, so a
value obfuscates the same way whether it appears in a document or a log line.

Age gate: the marking only exists for logs emitted after connectors shipped the
`logsanitize` change (see MARKING_RELEASED). Logs from before then have no
markers, so an absence of marked spans does NOT mean an absence of sensitive
data — obfuscating them would produce output that looks safe but isn't. The
script therefore refuses any log line it cannot confirm postdates the release.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone

from obfuscate_common import obfuscate_string

# The date the logsanitize markers were deployed to production connectors. Logs
# emitted before this cannot be trusted to mark their sensitive values.
# IMPORTANT: update this to the actual production release/deploy date.
MARKING_RELEASED = "2026-07-16T00:00:00Z"

# Timestamp fields to look for, in priority order. Flow ops logs use "ts";
# raw logrus JSON uses "time"; others are accepted as fallbacks.
TIMESTAMP_FIELDS = ("ts", "time", "timestamp", "@ts", "@timestamp")

OPEN = ""
CLOSE = ""

# Non-greedy so adjacent marked spans on one line are matched independently.
MARKED = re.compile(f"{OPEN}(.*?){CLOSE}", re.DOTALL)


def _obfuscate_marked(inner: str, salt: str) -> str:
    # `logsanitize.Quoted` wraps the value in Go quotes; strip a surrounding pair
    # so a quoted date-time still obfuscates via the date-time path (and so the
    # result matches obfuscate_jsonl.py for the same underlying value).
    if len(inner) >= 2 and inner[0] == '"' and inner[-1] == '"':
        return '"' + obfuscate_string(inner[1:-1], salt) + '"'
    return obfuscate_string(inner, salt)


def _sub_markers(text: str, mode: str, salt: str) -> str:
    def repl(m: re.Match) -> str:
        if mode == "redact":
            return "«redacted»"
        out = _obfuscate_marked(m.group(1), salt)
        return f"{OPEN}{out}{CLOSE}" if mode == "keep-markers" else out

    return MARKED.sub(repl, text)


def _walk_json(value: object, mode: str, salt: str) -> object:
    if isinstance(value, str):
        return _sub_markers(value, mode, salt)
    if isinstance(value, dict):
        return {k: _walk_json(v, mode, salt) for k, v in value.items()}
    if isinstance(value, list):
        return [_walk_json(v, mode, salt) for v in value]
    return value


def _parse_ts(s: str) -> datetime | None:
    normalised = s[:-1] + "+00:00" if s.endswith("Z") else s
    try:
        dt = datetime.fromisoformat(normalised)
    except ValueError:
        return None
    # Treat a naive timestamp as UTC so comparison against the cutoff is defined.
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


def _line_timestamp(body: str) -> datetime | None:
    try:
        obj = json.loads(body)
    except ValueError:
        return None
    if not isinstance(obj, dict):
        return None
    for field in TIMESTAMP_FIELDS:
        value = obj.get(field)
        if isinstance(value, str) and (dt := _parse_ts(value)) is not None:
            return dt
    return None


def check_ages(lines: list[str], cutoff: datetime, source: str) -> None:
    """Refuse the batch unless every non-empty line is confirmably post-release.

    Runs before any output is written, so a rejected batch never yields a
    partial file that looks safe.
    """
    for n, line in enumerate(lines, 1):
        body = line.strip()
        if not body:
            continue
        dt = _line_timestamp(body)
        if dt is None:
            raise SystemExit(
                f"{source}:{n}: could not determine a log timestamp, so these logs cannot be "
                f"confirmed to postdate the sensitive-data marking release ({MARKING_RELEASED}). "
                "They are not clearly marked with sensitive information and as such cannot be "
                "safely obfuscated. Re-run with --skip-age-check only if you are certain the logs "
                "are post-release."
            )
        if dt < cutoff:
            raise SystemExit(
                f"{source}:{n}: log timestamp {dt.isoformat()} predates the sensitive-data "
                f"marking release ({MARKING_RELEASED}). These logs are not clearly marked with "
                "sensitive information and as such cannot be safely obfuscated."
            )


def process_line(line: str, mode: str, salt: str) -> str:
    # Fast path: lines with no marked value pass through byte-identical.
    if OPEN not in line:
        return line

    body = line.rstrip("\n")
    newline = line[len(body):]

    # JSON logs (logrus): decode so markers sit in real string values with
    # unescaped quotes and clean date-times, then re-encode. This keeps the
    # output valid JSON regardless of what obfuscation produces.
    try:
        obj = json.loads(body)
    except ValueError:
        return _sub_markers(line, mode, salt)  # plain-text log line

    obfuscated = _walk_json(obj, mode, salt)
    return json.dumps(obfuscated, ensure_ascii=False, separators=(",", ":")) + newline


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("log", help="Input log file ('-' for stdin)")
    parser.add_argument("-o", "--output", help="Output file (default: stdout)")
    parser.add_argument("--mode", choices=["obfuscate", "redact", "keep-markers"], default="obfuscate")
    parser.add_argument("--salt", default="obfuscate", help="Salt for deterministic obfuscation (default: 'obfuscate')")
    parser.add_argument("--skip-age-check", action="store_true",
                        help="Bypass the age gate. UNSAFE: only for logs you know postdate the marking release.")
    args = parser.parse_args()

    source = "<stdin>" if args.log == "-" else args.log
    infile = sys.stdin if args.log == "-" else open(args.log)
    try:
        lines = infile.readlines()
    finally:
        if infile is not sys.stdin:
            infile.close()

    if not args.skip_age_check:
        cutoff = _parse_ts(MARKING_RELEASED)
        if cutoff is None:
            raise SystemExit(f"MARKING_RELEASED is not a valid RFC3339 timestamp: {MARKING_RELEASED!r}")
        # Validate the whole batch before writing anything, so a refusal never
        # leaves a partial output file that appears fully obfuscated.
        check_ages(lines, cutoff, source)

    outfile = open(args.output, "w") if args.output else sys.stdout
    try:
        for line in lines:
            outfile.write(process_line(line, args.mode, args.salt))
    finally:
        if outfile is not sys.stdout:
            outfile.close()


if __name__ == "__main__":
    main()
