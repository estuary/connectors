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

# Ensure the connector is available in the selected mode.
if (( DOCKER )); then
  IMAGE="ghcr.io/estuary/${CONNECTOR}:local"
  if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "image $IMAGE not found; building with build-local.sh"
    (cd "$ROOT_DIR" && ./build-local.sh "$CONNECTOR")
  fi
else
  CONNECTOR_BIN="$ROOT_DIR/$CONNECTOR/connector"
  if [[ ! -x "$CONNECTOR_BIN" ]]; then
    echo "building connector binary: $CONNECTOR_BIN"
    (cd "$ROOT_DIR" && go build -v -tags nozstd -o "$CONNECTOR_BIN" ./$CONNECTOR/...)
  fi
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
  cleanup() {
    if (( ! KEEP )); then
      echo "tearing down docker-compose"
      docker compose -f "$COMPOSE_FILE" down -v || true
    else
      echo "--keep set; leaving docker-compose running"
    fi
  }
  trap cleanup EXIT
  # Same grace period as tests/materialize/<connector>/setup.sh.
  sleep 5
else
  echo "no docker-compose for $CONNECTOR; assuming endpoint is reachable"
fi

# Render the flow spec.
SPEC="$OUT_DIR/flow.yaml"
SPEC_ARGS=(
  --scenario "$SCENARIO"
  --connector "$CONNECTOR"
  --config    "$CONFIG"
  --output    "$SPEC"
)
if (( DOCKER )); then
  SPEC_ARGS+=(--docker)
fi
echo "rendering spec: $SPEC"
python3 "$BENCH_DIR/generate_spec.py" "${SPEC_ARGS[@]}"

TASK="bench/$CONNECTOR"

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

START_NS=$(python3 -c "import time; print(time.monotonic_ns())")
set +e
flowctl preview "${PREVIEW_ARGS[@]}" >"$OUT_DIR/preview.stdout" 2>"$PREVIEW_LOG"
PREVIEW_RC=$?
set -e
END_NS=$(python3 -c "import time; print(time.monotonic_ns())")
WALL_S=$(python3 -c "print(($END_NS - $START_NS) / 1e9)")

# If the generator is still alive (e.g., preview died early), it's blocked
# on writing to a FIFO no one reads. Kill it so we don't hang.
if kill -0 $GEN_PID 2>/dev/null; then
  kill $GEN_PID 2>/dev/null || true
fi
wait $GEN_PID 2>/dev/null || true

echo "preview exit: $PREVIEW_RC"
echo "wall time:    ${WALL_S}s"

# Build a results.json from the state log + wall time. Per-transaction
# timing requires parsing flowctl preview's stderr (which logs each
# commit boundary); for now we report aggregate wall time and per-tx
# planned bytes from the state log. Per-tx wall time is a follow-up.
python3 - "$STATE_LOG" "$WALL_S" "$PREVIEW_RC" "$CONNECTOR" "$SCENARIO" "$SEED" >"$OUT_DIR/results.json" <<'PY'
import json, sys, os
state_path, wall, rc, connector, scenario, seed = sys.argv[1:7]
with open(state_path) as f:
    state = json.load(f)

txs = []
total_docs = 0
total_bytes = 0
for tx in state["transactions"]:
    bytes_ = tx["doc_count"] * tx["doc_size"]
    total_docs  += tx["doc_count"]
    total_bytes += bytes_
    txs.append({
        "index": tx["index"],
        "collection": tx["collection"],
        "doc_count": tx["doc_count"],
        "doc_size":  tx["doc_size"],
        "bytes":     bytes_,
        "fresh":     tx["fresh"],
        "overlaps":  tx["overlaps"],
    })

wall = float(wall)
print(json.dumps({
    "connector": connector,
    "scenario":  os.path.basename(scenario),
    "seed":      int(seed),
    "preview_exit_code": int(rc),
    "wall_seconds": wall,
    "total_docs":   total_docs,
    "total_bytes":  total_bytes,
    "docs_per_sec": (total_docs  / wall) if wall > 0 else None,
    "mb_per_sec":   (total_bytes / wall / (1024*1024)) if wall > 0 else None,
    "transactions": txs,
}, indent=2))
PY

echo "results: $OUT_DIR/results.json"

# Pretty-print the results table.
python3 "$BENCH_DIR/print_results.py" "$OUT_DIR/results.json" || true

exit $PREVIEW_RC
