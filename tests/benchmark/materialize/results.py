"""Build one benchmark run's results.json from its state log and flowctl log.

Usage:
  results.py STATE_LOG PREVIEW_RC CONNECTOR SCENARIO SEED PREVIEW_LOG

The document goes to stdout; run.sh redirects it to <out-dir>/results.json.
boundaries_from_log is module level so test_results.py can exercise it
against log excerpts.
"""

import json
import os
import re
import sys
from datetime import datetime


def boundaries_from_log(path):
    """Derive per-transaction boundaries from the preview's own log.

    flowctl publishes one "transaction stats" line per round, at info, from
    the process itself rather than the task's log stream, so it survives
    whatever `shards.logLevel` and `RUST_LOG` a run sets. Consecutive lines
    bound a transaction.

    Two properties of a run make the round count and the transaction count
    differ by one, in opposite directions:

    - Round 0 is the fixture's leading bare {"commit": true}. It reads no store
      requests and commits in milliseconds, so it is the left edge of the first
      transaction rather than a transaction of its own.
    - The leader writes round N's ACK intents at the start of round N+1, and
      the actor stops as soon as the last round is durable. The final
      transaction therefore has no ACK line, though its data is committed.

    Pairing the N+1 stats lines gives exactly N bounds, one per transaction,
    which is why they are read in preference to the ACK line.

    Returns (apply_seconds, [(start_ns, end_ns)], transactions_observed).
    """
    ansi = re.compile("\x1b\\[[0-9;]*m")
    stamp_re = re.compile(r"(\d{4}-\d{2}-\d{2}T[\d:.]+Z)")
    rounds = []
    first_ts = None
    with open(path, errors="replace") as f:
        for line in f:
            line = ansi.sub("", line)
            st = stamp_re.search(line)
            if not st:
                continue
            stamp = st.group(1).rstrip("Z")
            if "." not in stamp:
                stamp += ".0"
            cur = int(datetime.strptime(
                stamp[:26], "%Y-%m-%dT%H:%M:%S.%f").timestamp() * 1e9)
            if first_ts is None:
                first_ts = cur
            if "preview_next::publish: transaction stats" in line:
                rounds.append(cur)

    if len(rounds) < 2:
        return None, [], 0
    apply_s = (rounds[0] - first_ts) / 1e9
    bounds = [(rounds[i - 1], rounds[i]) for i in range(1, len(rounds))]
    return apply_s, bounds, len(bounds)


def main(argv):
    state_path, rc, connector, scenario, seed, preview_log = argv[1:7]
    with open(state_path) as f:
        state = json.load(f)

    apply_seconds = None
    tx_boundaries = []  # (start_ns, end_ns) per data transaction
    rounds_observed = 0
    if os.path.exists(preview_log):
        apply_seconds, tx_boundaries, rounds_observed = \
            boundaries_from_log(preview_log)

    txs = []
    total_docs = 0
    total_bytes = 0
    # Throughput must divide by what actually ran. A run can cover fewer
    # transactions than the scenario describes, and dividing the scenario's
    # totals by the observed wall overstates the result.
    observed_docs = 0
    observed_bytes = 0
    for i, tx in enumerate(state["transactions"]):
        bytes_ = tx["doc_count"] * tx["doc_size"]
        total_docs  += tx["doc_count"]
        total_bytes += bytes_
        entry = {
            "index": tx["index"],
            "collection": tx["collection"],
            "doc_count": tx["doc_count"],
            "doc_size":  tx["doc_size"],
            "bytes":     bytes_,
            "fresh":     tx["fresh"],
            "overlaps":  tx["overlaps"],
        }
        if i < len(tx_boundaries):
            observed_docs += tx["doc_count"]
            observed_bytes += bytes_
            start_ns, end_ns = tx_boundaries[i]
            wall_s = (end_ns - start_ns) / 1e9
            entry["wall_seconds"] = wall_s
            entry["mb_per_sec"] = (bytes_ / wall_s / (1024*1024)) if wall_s > 0 else None
            entry["docs_per_sec"] = (tx["doc_count"] / wall_s) if wall_s > 0 else None
        txs.append(entry)

    # Data wall = end of Apply to end of last data tx (excludes Apply).
    if tx_boundaries:
        data_wall = (tx_boundaries[-1][1] - tx_boundaries[0][0]) / 1e9
    else:
        data_wall = 0.0

    print(json.dumps({
        "connector": connector,
        "scenario":  os.path.basename(scenario),
        "seed":      int(seed),
        "preview_exit_code": int(rc),
        "apply_seconds": apply_seconds,
        "wall_seconds": data_wall,
        "rounds_observed": rounds_observed,
        "scenario_transactions": len(state["transactions"]),
        "total_docs":   total_docs,
        "total_bytes":  total_bytes,
        "observed_docs":  observed_docs,
        "observed_bytes": observed_bytes,
        "docs_per_sec": (observed_docs  / data_wall) if data_wall > 0 else None,
        "mb_per_sec":   (observed_bytes / data_wall / (1024*1024)) if data_wall > 0 else None,
        "transactions": txs,
    }, indent=2))


if __name__ == "__main__":
    main(sys.argv)
