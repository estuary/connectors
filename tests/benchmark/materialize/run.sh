#!/bin/bash
#
# Materialization benchmark runner.
#
# Drives a materialization connector with a configured-size workload
# expressed as a scenario YAML. The scenario defines collections and a
# sequence of transactions (with optional overlap fractions for updates
# and deletes against earlier transactions). Documents are generated to
# hit precise byte sizes, so a "10 GB" transaction really moves 10 GB
# through the pipeline.
#
# By default the connector runs as a local binary (same as integration
# tests), so the existing testdata/config.local.yaml works out of the box.
# Pass --docker to run the connector as a Docker image instead.
#
# Usage:
#   tests/benchmark/materialize/run.sh \
#     --connector materialize-postgres \
#     --scenario  tests/benchmark/materialize/scenarios/large-3tx.yaml \
#     [--config <path>]      # default: <connector>/testdata/config.local.yaml
#                            #   or    <connector>/testdata/config.yaml
#     [--docker]             # run connector as Docker image (default: local binary)
#     [--seed N]             # default: 0
#     [--keep]               # don't tear down docker-compose on exit
#     [--out-dir DIR]        # default: tests/benchmark/materialize/runs/<ts>

set -o errexit
set -o pipefail
set -o nounset

export ROOT_DIR="$(git rev-parse --show-toplevel)"
BENCH_DIR="$ROOT_DIR/tests/benchmark/materialize"

CONNECTOR=""
SCENARIO=""
CONFIG=""
SEED=0
KEEP=0
OUT_DIR=""
DOCKER=0

while (($#)); do
  case "$1" in
    --connector) CONNECTOR="$2"; shift 2 ;;
    --scenario)  SCENARIO="$2";  shift 2 ;;
    --config)    CONFIG="$2";    shift 2 ;;
    --seed)      SEED="$2";      shift 2 ;;
    --keep)      KEEP=1;         shift   ;;
    --docker)    DOCKER=1;       shift   ;;
    --out-dir)   OUT_DIR="$2";   shift 2 ;;
    -h|--help)
      sed -n '2,26p' "$0"; exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

[[ -n "$CONNECTOR" ]] || { echo "--connector is required" >&2; exit 2; }
[[ -n "$SCENARIO" ]]  || { echo "--scenario is required"  >&2; exit 2; }
[[ -f "$SCENARIO" ]]  || { echo "scenario not found: $SCENARIO" >&2; exit 2; }

command -v flowctl >/dev/null || { echo "flowctl not on PATH" >&2; exit 2; }
command -v python3 >/dev/null || { echo "python3 not on PATH" >&2; exit 2; }
python3 -c "import yaml" 2>/dev/null || {
  echo "PyYAML required: pip3 install pyyaml" >&2; exit 2;
}

# Resolve config: explicit, then config.local.yaml, then config.yaml.
if [[ -z "$CONFIG" ]]; then
  for cand in \
    "$ROOT_DIR/$CONNECTOR/testdata/config.local.yaml" \
    "$ROOT_DIR/$CONNECTOR/testdata/config.yaml"
  do
    if [[ -f "$cand" ]]; then CONFIG="$cand"; break; fi
  done
fi
[[ -n "$CONFIG" && -f "$CONFIG" ]] || {
  echo "no config found; pass --config <path> or place one at" >&2
  echo "  $CONNECTOR/testdata/config.local.yaml" >&2
  exit 2
}

# Always rebuild the connector so each run picks up the latest code.
if (( DOCKER )); then
  echo "building docker image for $CONNECTOR"
  (cd "$ROOT_DIR" && ./build-local.sh "$CONNECTOR")
else
  CONNECTOR_BIN="$ROOT_DIR/$CONNECTOR/connector"
  # Connectors structured as a library plus a cmd/ main (all the SQL ones)
  # match more than one package under ./<connector>/..., which `go build -o
  # <file>` refuses. Prefer the cmd package when there is one.
  if [[ -d "$ROOT_DIR/$CONNECTOR/cmd" ]]; then
    BUILD_PKG="./$CONNECTOR/cmd/..."
  else
    BUILD_PKG="./$CONNECTOR/..."
  fi
  echo "building connector binary: $CONNECTOR_BIN (from $BUILD_PKG)"
  (cd "$ROOT_DIR" && go build -v -tags nozstd -o "$CONNECTOR_BIN" "$BUILD_PKG")
fi

# Output directory.
if [[ -z "$OUT_DIR" ]]; then
  OUT_DIR="$BENCH_DIR/runs/$(date +%Y%m%d-%H%M%S)-${CONNECTOR}"
fi
mkdir -p "$OUT_DIR"

echo "connector: $CONNECTOR"
echo "scenario:  $SCENARIO"
echo "config:    $CONFIG"
if (( DOCKER )); then echo "mode:      docker"; else echo "mode:      local"; fi
echo "seed:      $SEED"
echo "out-dir:   $OUT_DIR"

# Every run gets its own task name. A task's stored checkpoint otherwise
# carries over between runs. The _flow_test_<timestamp> shape is what
# `testctl -mode sweep` recognises, so a run that has been killed can still
# be cleaned up afterwards.
RUN_SUFFIX="_flow_test_$(date +%s)"
TASK="bench/${CONNECTOR}${RUN_SUFFIX}"
echo "task:      $TASK"

# Bring up the connector's docker-compose stack (DB only). Some connectors
# keep it under tests/materialize/<connector>/ (the integration-test convention),
# others keep it at the connector root. Try both.
COMPOSE_FILE=""
for cand in \
  "$ROOT_DIR/tests/materialize/$CONNECTOR/docker-compose.yaml" \
  "$ROOT_DIR/$CONNECTOR/docker-compose.yaml"
do
  if [[ -f "$cand" ]]; then COMPOSE_FILE="$cand"; break; fi
done
if [[ -n "$COMPOSE_FILE" ]]; then
  echo "starting docker-compose: $COMPOSE_FILE"
  docker compose -f "$COMPOSE_FILE" up --wait
  # Same grace period as tests/materialize/<connector>/setup.sh.
  sleep 5
else
  echo "no docker-compose for $CONNECTOR; assuming endpoint is reachable"
fi

# clean up the destination resources using testctl
TESTCTL="$OUT_DIR/testctl"
echo "building testctl"
(cd "$ROOT_DIR" && go build -tags nozstd -o "$TESTCTL" ./tests/materialize/testctl)

drop_destination() {
  local when="$1" res
  for res in "$OUT_DIR"/resource-*.json; do
    [[ -f "$res" ]] || continue
    if "$TESTCTL" -connector "$CONNECTOR" -config "$CONFIG" -resource "$res" \
         -task "$TASK" -mode drop >>"$OUT_DIR/cleanup.log" 2>&1; then
      echo "  dropped $(basename "$res" .json) ($when)"
    else
      echo "  no $(basename "$res" .json) to drop ($when)"
    fi
  done
}

sweep_run() {
  local res
  for res in "$OUT_DIR"/resource-*.json; do
    [[ -f "$res" ]] || continue
    "$TESTCTL" -connector "$CONNECTOR" -config "$CONFIG" -resource "$res" \
      -task "$TASK" -mode sweep -run-suffix "$RUN_SUFFIX" \
      >>"$OUT_DIR/cleanup.log" 2>&1 || echo "  sweep reported errors (see cleanup.log)"
    return
  done
}

cleanup() {
  local status=$?
  if (( KEEP )); then
    echo "--keep set; leaving the destination and docker-compose in place"
  else
    echo "cleaning up"
    drop_destination "teardown"
    sweep_run
    if [[ -n "$COMPOSE_FILE" ]]; then
      echo "tearing down docker-compose"
      docker compose -f "$COMPOSE_FILE" down -v || true
    fi
  fi
  return $status
}
trap cleanup EXIT INT TERM

# Per-connector setup hook. Runs after docker-compose is up but before the
# preview. Used for steps like creating an S3 bucket that the connector
# expects to exist but does not create itself.
SETUP_SCRIPT="$BENCH_DIR/setup/${CONNECTOR}.sh"
if [[ -x "$SETUP_SCRIPT" ]]; then
  echo "running setup hook: $SETUP_SCRIPT"
  BENCH_CONFIG="$CONFIG" "$SETUP_SCRIPT"
fi

# Render the flow spec.
SPEC="$OUT_DIR/flow.yaml"
SPEC_ARGS=(
  --scenario "$SCENARIO"
  --connector "$CONNECTOR"
  --config    "$CONFIG"
  --output    "$SPEC"
  --task      "$TASK"
)
if (( DOCKER )); then
  SPEC_ARGS+=(--docker)
fi
echo "rendering spec: $SPEC"
python3 "$BENCH_DIR/generate_spec.py" "${SPEC_ARGS[@]}"

# One resource configuration file per binding, for testctl.
python3 - "$SPEC" "$OUT_DIR" <<'RESOURCES'
import json, sys, yaml

spec_path, out_dir = sys.argv[1], sys.argv[2]
with open(spec_path) as f:
    spec = yaml.safe_load(f)
materialization = next(iter(spec["materializations"].values()))
for index, binding in enumerate(materialization["bindings"]):
    with open(f"{out_dir}/resource-{index}.json", "w") as f:
        json.dump(binding["resource"], f)
RESOURCES

# Drop whatever an earlier run left behind, so this run starts against an empty
# destination however that run ended.
echo "preparing destination"
drop_destination "before run"

# Set up FIFO + background generator.
FIFO="$OUT_DIR/fixture.fifo"
[[ -p "$FIFO" ]] || mkfifo "$FIFO"

STATE_LOG="$OUT_DIR/state.json"
echo "starting generator (seed=$SEED)"
python3 "$BENCH_DIR/generate.py" \
  --scenario "$SCENARIO" \
  --seed     "$SEED" \
  --output   "$FIFO" \
  --state    "$STATE_LOG" \
  >"$OUT_DIR/generator.stderr" 2>&1 &
GEN_PID=$!

# Run flowctl preview, timing the whole thing.
PREVIEW_LOG="$OUT_DIR/preview.log"
echo "running flowctl preview (logs: $PREVIEW_LOG)"

PREVIEW_ARGS=(
  --source  "$SPEC"
  --name    "$TASK"
  --fixture "$FIFO"
  --output-state
)
# Docker mode needs --network so the connector container can reach the DB.
if (( DOCKER )); then
  PREVIEW_ARGS+=(--network flow-test)
fi

# Pipe preview stdout through a timestamper. Each connectorState line from
# flowctl marks a commit boundary. The first is Apply (DDL/table creation),
# subsequent ones are data transactions. The timestamper records monotonic
# nanosecond timestamps so we can compute per-transaction wall times and
# exclude Apply from the data throughput measurement.
TIMESTAMPS="$OUT_DIR/timestamps.json"
set +e
flowctl preview "${PREVIEW_ARGS[@]}" 2>"$PREVIEW_LOG" \
  | python3 -c "
import sys, time, json
ts = [time.monotonic_ns()]  # ts[0] = start (before any data)
for line in sys.stdin:
    sys.stdout.write(line)
    sys.stdout.flush()
    if '\"connectorState\"' in line:
        ts.append(time.monotonic_ns())
with open('$TIMESTAMPS', 'w') as f:
    json.dump(ts, f)
" >"$OUT_DIR/preview.stdout"
PREVIEW_RC=${PIPESTATUS[0]}
set -e

# If the generator is still alive (e.g., preview died early), it's blocked
# on writing to a FIFO no one reads. Kill it so we don't hang.
if kill -0 $GEN_PID 2>/dev/null; then
  kill $GEN_PID 2>/dev/null || true
fi
wait $GEN_PID 2>/dev/null || true

echo "preview exit: $PREVIEW_RC"

# Build results.json from state log + per-commit timestamps.
# timestamps.json has one entry per connectorState line: the first is Apply
# (DDL), the rest are data transactions.
python3 - "$STATE_LOG" "$TIMESTAMPS" "$PREVIEW_RC" "$CONNECTOR" "$SCENARIO" "$SEED" "$PREVIEW_LOG" >"$OUT_DIR/results.json" <<'PY'
import json, sys, os, re
from datetime import datetime
state_path, ts_path, rc, connector, scenario, seed = sys.argv[1:7]
preview_log = sys.argv[7] if len(sys.argv) > 7 else None
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
                cur = int(
                    datetime.strptime(
                        st.group(1)[:26].ljust(26, "0"), "%Y-%m-%dT%H:%M:%S.%f"
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


rounds_observed = len(tx_boundaries)
timing_source = "connector-state"
if preview_log and os.path.exists(preview_log):
    log_apply, log_bounds, log_rounds = boundaries_from_log(preview_log)
    if log_bounds:
        apply_seconds, tx_boundaries = log_apply, log_bounds
        rounds_observed, timing_source = log_rounds, "flowctl-log"

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
if timing_source == "flowctl-log" and tx_boundaries:
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
PY

echo "results: $OUT_DIR/results.json"

# Pretty-print the results table.
python3 "$BENCH_DIR/print_results.py" "$OUT_DIR/results.json" || true

exit $PREVIEW_RC
