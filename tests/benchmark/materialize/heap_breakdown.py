#!/usr/bin/env python3
"""Attribute sampled heap profiles (from sample_heap.sh) to memory categories.

    ./heap_breakdown.py OUT_DIR CONNECTOR_BINARY [--all]

Runs `go tool pprof -top` on every heap-*.pb.gz sample, sums in-use bytes
per category by function name, and prints one row per sample plus the peak
of each category. Categories:

  upload  aws transfer manager / gosnowflake storage clients (PUT buffers)
  gzip    pgzip / flate compression writers
  encode  go/writer JSON encoding and row conversion
  other   everything else
"""
import glob
import os
import re
import subprocess
import sys

CATEGORIES = [
    ("upload", re.compile(r"transfermanager|gosnowflake.*(s3|azure|gcs|storage|encrypt|upload|fileTransfer)|aws-sdk|aws/")),
    ("gzip", re.compile(r"pgzip|compress/flate|compress/gzip")),
    ("encode", re.compile(r"connectors/go/writer|encrow|json\.|ConvertAll|materialize-sql")),
]

UNITS = {"B": 1, "kB": 1 << 10, "MB": 1 << 20, "GB": 1 << 30, "TB": 1 << 40}
LINE = re.compile(r"^\s*([\d.]+)(B|kB|MB|GB|TB)\s+[\d.]+%\s+[\d.]+%\s+([\d.]+)(B|kB|MB|GB|TB)\s+[\d.]+%\s+(.*)$")


def classify(name: str) -> str:
    for cat, rx in CATEGORIES:
        if rx.search(name):
            return cat
    return "other"


def breakdown(binary: str, sample: str) -> dict[str, int]:
    out = subprocess.run(
        ["go", "tool", "pprof", "-top", "-nodecount=100000", "-sample_index=inuse_space", binary, sample],
        capture_output=True, text=True, check=True,
    ).stdout
    sums = {"upload": 0, "gzip": 0, "encode": 0, "other": 0}
    for line in out.splitlines():
        m = LINE.match(line)
        if not m:
            continue
        flat = float(m.group(1)) * UNITS[m.group(2)]
        sums[classify(m.group(5))] += int(flat)
    return sums


def memstats(path: str) -> dict[str, int]:
    stats = {}
    if not os.path.exists(path):
        return stats
    for line in open(path):
        parts = line.lstrip("# ").split("=")
        if len(parts) == 2:
            stats[parts[0].strip()] = int(parts[1])
    return stats


def mib(n: int) -> str:
    return f"{n / (1 << 20):7.0f}"


def main() -> int:
    if len(sys.argv) < 3:
        print(__doc__)
        return 2
    out_dir, binary = sys.argv[1], sys.argv[2]
    show_all = "--all" in sys.argv
    rss = {}
    rss_path = os.path.join(out_dir, "rss.log")
    if os.path.exists(rss_path):
        for line in open(rss_path):
            ts, kv = line.split()
            rss[ts] = int(kv.split("=")[1]) << 10

    samples = sorted(glob.glob(os.path.join(out_dir, "heap-*.pb.gz")))
    if not samples:
        print("no heap samples in", out_dir)
        return 1

    rows = []
    for s in samples:
        ts = re.search(r"heap-(\d+)", s).group(1)
        b = breakdown(binary, s)
        ms = memstats(os.path.join(out_dir, f"memstats-{ts}.txt"))
        rows.append((ts, b, ms.get("HeapInuse", 0), ms.get("Sys", 0), rss.get(ts, 0)))

    print(f"{'ts':>10} {'upload':>8} {'gzip':>8} {'encode':>8} {'other':>8} {'inuse':>8} {'HeapIn':>8} {'Sys':>8} {'RSS':>8}  (MiB)")
    peak = {k: 0 for k in ("upload", "gzip", "encode", "other", "inuse", "HeapInuse", "Sys", "RSS")}
    peak_row = None
    for ts, b, heap_inuse, sys_bytes, r in rows:
        total = sum(b.values())
        for k in ("upload", "gzip", "encode", "other"):
            peak[k] = max(peak[k], b[k])
        peak["inuse"] = max(peak["inuse"], total)
        peak["HeapInuse"] = max(peak["HeapInuse"], heap_inuse)
        peak["Sys"] = max(peak["Sys"], sys_bytes)
        peak["RSS"] = max(peak["RSS"], r)
        if peak_row is None or total > peak_row[1]:
            peak_row = (ts, total, b, heap_inuse, sys_bytes, r)
        if show_all:
            print(f"{ts:>10} {mib(b['upload'])} {mib(b['gzip'])} {mib(b['encode'])} {mib(b['other'])} {mib(total)} {mib(heap_inuse)} {mib(sys_bytes)} {mib(r)}")

    print()
    print("peak per column (not simultaneous):")
    print(f"{'':>10} {mib(peak['upload'])} {mib(peak['gzip'])} {mib(peak['encode'])} {mib(peak['other'])} {mib(peak['inuse'])} {mib(peak['HeapInuse'])} {mib(peak['Sys'])} {mib(peak['RSS'])}")
    ts, total, b, heap_inuse, sys_bytes, r = peak_row
    print("sample with the largest in-use heap:")
    print(f"{ts:>10} {mib(b['upload'])} {mib(b['gzip'])} {mib(b['encode'])} {mib(b['other'])} {mib(total)} {mib(heap_inuse)} {mib(sys_bytes)} {mib(r)}")
    print(f"samples: {len(rows)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
