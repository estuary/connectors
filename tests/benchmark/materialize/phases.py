"""Per-round connector phase timings from a run's preview.log.

Usage:
  phases.py PREVIEW_LOG [PREVIEW_LOG ...]

Prints, per round, how long the connector spent evaluating loads (the load
query) and committing each resource (the COPY INTO / MERGE queries), from the
"finished evaluating loads" and "finished committing documents for resource"
lines the materialize boilerplate logs at info. Those lines only reach the
preview log when the run sets `--shard-log-level info`.

results.py measures whole transactions, which include the fixture feeder and
the connector's uploads. This isolates the warehouse-side work, which is what
a staging or query change moves.

A round's commit is logged while the next round runs, and the preview exits
before the final round's commit is logged, so the last round always shows 0.0
for commit. Give a scenario a small trailing transaction when the final data
transaction's commit matters.
"""

import re
import sys

ANSI = re.compile("\x1b\\[[0-9;]*m")
TOOK = re.compile(r'took["=:\s]+"?(\d+(?:\.\d+)?(?:h|m|s|ms|µs|us|ns)(?:\d+(?:\.\d+)?(?:m|s|ms|µs|us|ns))*)')
ROUND = re.compile(r'round["=:\s]+"?(\d+)')
UNITS = {"h": 3600, "m": 60, "s": 1, "ms": 1e-3, "µs": 1e-6, "us": 1e-6, "ns": 1e-9}


def parse_duration(text):
    """Go duration string ("1h2m3.5s", "250ms") to seconds."""
    total = 0.0
    for value, unit in re.findall(r"(\d+(?:\.\d+)?)(h|ms|m|s|µs|us|ns)", text):
        total += float(value) * UNITS[unit]
    return total


def phases_from_log(path):
    loads, commits = {}, {}
    with open(path, errors="replace") as f:
        for line in f:
            line = ANSI.sub("", line)
            if "finished evaluating loads" in line:
                target = loads
            elif "finished committing documents for resource" in line:
                target = commits
            else:
                continue
            took, rnd = TOOK.search(line), ROUND.search(line)
            if not took or not rnd:
                continue
            target.setdefault(int(rnd.group(1)), 0.0)
            target[int(rnd.group(1))] += parse_duration(took.group(1))
    return loads, commits


def main(argv):
    for path in argv[1:]:
        loads, commits = phases_from_log(path)
        rounds = sorted(set(loads) | set(commits))
        print(path)
        print(f"  {'round':>5}  {'load eval (s)':>14}  {'commit (s)':>11}")
        for r in rounds:
            print(f"  {r:>5}  {loads.get(r, 0.0):>14.1f}  {commits.get(r, 0.0):>11.1f}")
        print(f"  {'total':>5}  {sum(loads.values()):>14.1f}  {sum(commits.values()):>11.1f}")


if __name__ == "__main__":
    main(sys.argv)
