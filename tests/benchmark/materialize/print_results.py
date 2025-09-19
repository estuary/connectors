#!/usr/bin/env python3
"""
Pretty-print benchmark results.json to the terminal.

Usage:
    print_results.py <results.json>
    print_results.py <run-dir>          # looks for results.json inside

Adapts column widths to terminal size. Uses ANSI colors when stdout is
a TTY (disable with NO_COLOR=1 or piping to a file).
"""

from __future__ import annotations

import json
import os
import shutil
import sys


# ---------------------------------------------------------------------------
# ANSI helpers
# ---------------------------------------------------------------------------

def _use_color() -> bool:
    if os.environ.get("NO_COLOR"):
        return False
    return hasattr(sys.stdout, "isatty") and sys.stdout.isatty()

_COLOR = _use_color()

def _sgr(code: str) -> str:
    return f"\033[{code}m" if _COLOR else ""

RESET   = _sgr("0")
BOLD    = _sgr("1")
DIM     = _sgr("2")
GREEN   = _sgr("32")
YELLOW  = _sgr("33")
RED     = _sgr("31")
CYAN    = _sgr("36")
MAGENTA = _sgr("35")
WHITE   = _sgr("37")


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _human_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            if n == int(n):
                return f"{int(n)} {unit}"
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def _human_count(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)


def _human_duration(s: float) -> str:
    if s < 60:
        return f"{s:.1f}s"
    m = int(s) // 60
    sec = s - m * 60
    if m < 60:
        return f"{m}m {sec:.0f}s"
    h = m // 60
    m = m % 60
    return f"{h}h {m}m {sec:.0f}s"


def _overlap_summary(overlaps: list[dict]) -> str:
    if not overlaps:
        return DIM + "none" + RESET
    parts = []
    for o in overlaps:
        op_color = YELLOW if o["op"] == "u" else RED
        parts.append(f'{op_color}{o["count"]}{o["op"]}{RESET}{DIM}@tx{o["with"]}{RESET}')
    return " ".join(parts)


def _status_badge(rc: int) -> str:
    if rc == 0:
        return f"{GREEN}{BOLD} PASS {RESET}"
    return f"{RED}{BOLD} FAIL (exit {rc}) {RESET}"


# ---------------------------------------------------------------------------
# Table rendering
# ---------------------------------------------------------------------------

def _print_line(char: str, width: int) -> None:
    print(DIM + char * width + RESET)


def _visible_len(s: str) -> int:
    """Length of string ignoring ANSI escape sequences."""
    import re
    return len(re.sub(r"\033\[[0-9;]*m", "", s))


def _print_row_v2(cells: list[str], widths: list[int], aligns: list[str]) -> None:
    parts = []
    for cell, w, align in zip(cells, widths, aligns):
        pad = w - _visible_len(cell)
        if pad < 0:
            pad = 0
        if align == "r":
            parts.append(" " * pad + cell)
        else:
            parts.append(cell + " " * pad)
    print("  ".join(parts))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def print_results(data: dict) -> None:
    term_width = shutil.get_terminal_size((100, 24)).columns

    # Header
    print()
    _print_line("─", term_width)
    connector = data["connector"]
    scenario = data["scenario"]
    print(f"  {BOLD}{CYAN}{connector}{RESET}  {DIM}scenario={RESET}{scenario}  "
          f"{DIM}seed={RESET}{data['seed']}  {_status_badge(data['preview_exit_code'])}")
    _print_line("─", term_width)

    # Summary stats
    wall = data["wall_seconds"]
    print(
        f"  {BOLD}Wall time{RESET}   {_human_duration(wall)}"
        f"     {BOLD}Total data{RESET}  {_human_bytes(data['total_bytes'])}"
        f"     {BOLD}Docs{RESET}  {_human_count(data['total_docs'])}"
    )
    mb_s = data.get("mb_per_sec")
    docs_s = data.get("docs_per_sec")
    throughput_color = GREEN if (mb_s and mb_s > 10) else YELLOW if (mb_s and mb_s > 1) else RED
    print(
        f"  {BOLD}Throughput{RESET}   {throughput_color}{BOLD}"
        f"{mb_s:.1f} MB/s{RESET}"
        f"     {BOLD}Docs/sec{RESET}    {_human_count(int(docs_s)) if docs_s else 'N/A'}"
    )
    print()

    # Transaction table
    txs = data.get("transactions", [])
    if not txs:
        return

    # Build rows
    headers = ["Tx", "Collection", "Docs", "Doc Size", "Total", "Fresh", "Overlaps"]
    aligns  = ["r",  "l",          "r",    "r",        "r",     "r",     "l"]

    rows: list[list[str]] = []
    for tx in txs:
        fresh = tx["fresh"]
        rows.append([
            f'{BOLD}#{tx["index"]}{RESET}',
            f'{DIM}{tx["collection"]}{RESET}',
            _human_count(tx["doc_count"]),
            _human_bytes(tx["doc_size"]),
            _human_bytes(tx["bytes"]),
            f'{GREEN}{_human_count(fresh["count"])}{fresh["op"]}{RESET}',
            _overlap_summary(tx["overlaps"]),
        ])

    # Compute column widths (clamp to terminal).
    col_widths = [_visible_len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], _visible_len(cell))

    # Header row
    header_cells = [f"{BOLD}{h}{RESET}" for h in headers]
    _print_row_v2(header_cells, col_widths, aligns)
    _print_line("─", min(sum(col_widths) + 2 * (len(col_widths) - 1), term_width))

    # Data rows
    for row in rows:
        _print_row_v2(row, col_widths, aligns)

    print()


def main() -> int:
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <results.json | run-dir>", file=sys.stderr)
        return 2

    path = sys.argv[1]
    if os.path.isdir(path):
        path = os.path.join(path, "results.json")
    if not os.path.isfile(path):
        print(f"not found: {path}", file=sys.stderr)
        return 1

    with open(path) as f:
        data = json.load(f)

    print_results(data)
    return 0


if __name__ == "__main__":
    sys.exit(main())
