"""Build one benchmark run's results.json from its state log and flowctl log.

Usage:
  results.py STATE_LOG TIMESTAMPS PREVIEW_RC CONNECTOR SCENARIO SEED [PREVIEW_LOG]

The document goes to stdout; run.sh redirects it to <out-dir>/results.json.
The boundary functions are module level so test_results.py can exercise them
against log excerpts.
"""

import json
import os
import re
import sys
from datetime import datetime


def boundaries_from_log(path):
    """Derive per-round boundaries from flowctl's own log.

    The connectorState-line method assumes one state line for Apply and one
    per transaction. That holds only for connectors that emit a state document
    at Apply; a connector whose destination is authoritative emits none, so
    every boundary shifts by one and Apply absorbs the first transaction.

    Each data round is bracketed by "started reading load requests" and
    "finished materialization commit", both emitted once per round. Some
    records arrive interleaved into another line and without their own
    timestamp, so the most recent timestamp seen is used; the error is far
    below the resolution that matters here.

    Returns (apply_seconds, [(start_ns, end_ns)], rounds_observed).
    """
    ansi = re.compile("\x1b\\[[0-9;]*m|\\\\x1b\\[[0-9;]*m")
    stamp_re = re.compile(r"(\d{4}-\d{2}-\d{2}T[\d:.]+Z)")
    start_re = re.compile(r"started reading load requests\s+round=(-?\d+)")
    end_re = re.compile(r"finished materialization commit\s+round=(-?\d+)")

    starts, ends = {}, {}
    first_ts = last_ts = cur = None
    with open(path, errors="replace") as f:
        for line in f:
            line = ansi.sub("", line)
            st = stamp_re.search(line)
            if st:
                # Not every stamp carries a fraction: the Snowpipe sidecar
                # relays its core's logs as whole seconds. Those need a ".0"
                # appended, not right-padding to 26 characters, which would put
                # zeros after the "Z" and raise in strptime.
                stamp = st.group(1).rstrip("Z")
                if "." not in stamp:
                    stamp += ".0"
                cur = int(
                    datetime.strptime(
                        stamp[:26], "%Y-%m-%dT%H:%M:%S.%f"
                    ).timestamp() * 1e9
                )
                if first_ts is None:
                    first_ts = cur
                last_ts = cur
            if cur is None:
                continue
            for m in start_re.finditer(line):
                rnd = int(m.group(1))
                if rnd >= 0:
                    starts.setdefault(rnd, cur)
            for m in end_re.finditer(line):
                rnd = int(m.group(1))
                if rnd >= 0:
                    ends[rnd] = cur

    if not starts or first_ts is None:
        return None, [], 0
    rounds = sorted(starts)
    apply_s = (starts[rounds[0]] - first_ts) / 1e9
    bounds = [(starts[r], ends.get(r, last_ts)) for r in rounds]
    return apply_s, bounds, len(rounds)


def boundaries_from_log_v2(path):
    """Derive per-transaction boundaries from `raw preview-next`'s own log.

    `raw preview-next` emits neither the ["connectorState",...] lines nor V1's
    `round=` lines. flowctl publishes one "transaction stats" line per round,
    at info, from the process itself rather than the task's log stream, so it
    survives whatever `shards.logLevel` and `RUST_LOG` a run sets. Consecutive
    lines bound a transaction, as consecutive commits do under V1.

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
    state_path, ts_path, rc, connector, scenario, seed = argv[1:7]
    preview_log = argv[7] if len(argv) > 7 else None
    with open(state_path) as f:
        state = json.load(f)
    try:
        with open(ts_path) as f:
            timestamps = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        timestamps = []

    # timestamps[0] = preview start, timestamps[1] = end of Apply (DDL),
    # timestamps[2..N] = end of each data transaction.
    apply_seconds = None
    tx_boundaries = []  # (start_ns, end_ns) per data transaction
    if len(timestamps) >= 2:
        apply_seconds = (timestamps[1] - timestamps[0]) / 1e9
        for i in range(2, len(timestamps)):
            tx_boundaries.append((timestamps[i - 1], timestamps[i]))



    rounds_observed = len(tx_boundaries)
    timing_source = "connector-state"
    if preview_log and os.path.exists(preview_log):
        log_apply, log_bounds, log_rounds = boundaries_from_log(preview_log)
        if log_bounds:
            apply_seconds, tx_boundaries = log_apply, log_bounds
            rounds_observed, timing_source = log_rounds, "flowctl-log"
        else:
            v2_apply, v2_bounds, v2_rounds = boundaries_from_log_v2(preview_log)
            if v2_bounds:
                apply_seconds, tx_boundaries = v2_apply, v2_bounds
                rounds_observed, timing_source = v2_rounds, "runtime-v2-log"

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
    if timing_source in ("flowctl-log", "runtime-v2-log") and tx_boundaries:
        data_wall = (tx_boundaries[-1][1] - tx_boundaries[0][0]) / 1e9
    elif len(timestamps) >= 3:
        data_wall = (timestamps[-1] - timestamps[1]) / 1e9
    else:
        data_wall = 0.0

    print(json.dumps({
        "connector": connector,
        "scenario":  os.path.basename(scenario),
        "seed":      int(seed),
        "preview_exit_code": int(rc),
        "apply_seconds": apply_seconds,
        "wall_seconds": data_wall,
        "timing_source": timing_source,
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
