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
#     [--shard-flag K=V]     # task shard flag, repeatable. Needed where a
#                            #   connector gates a write path on the runtime,
#                            #   e.g. enable-runtime-v2=true.
#     [--table-suffix S]     # appended to every binding's table name, so two
#                            #   arms of one comparison can run the same
#                            #   scenario against separate tables.
#     [--shard-log-level L]  # task shards.logLevel, e.g. debug. The runtime
#                            #   forces the connector's LOG_LEVEL from this, so
#                            #   exporting LOG_LEVEL has no effect.
#     [--shards N]           # default: 1. Drives N synthetic shards in one
#                            #   process. Fixture documents hash-route by
#                            #   collection key, so each document lands on
#                            #   exactly one shard.
#
#     [--preview-arg ARG]    # extra argument to the materialization's
#                            #   `flowctl raw preview-next`, repeatable.
#     [--tpcds-cache DIR]    # TPC-DS only: generate the fixture into DIR once
#                            #   before the run and replay it, here and in
#                            #   later runs of the same workload.
#
# A scenario with a `tpcds` block streams the TPC-DS dataset from the
# source-tpc-ds capture instead of synthetic documents (see tpcds.py). Its
# image is $TPCDS_IMAGE, default ghcr.io/estuary/source-tpc-ds:dev.

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
SHARD_FLAGS=()
TABLE_SUFFIX=""
SHARD_LOG_LEVEL=""
SHARDS=1
TPCDS_CACHE=""
PREVIEW_EXTRA=()

while (($#)); do
  case "$1" in
    --connector) CONNECTOR="$2"; shift 2 ;;
    --scenario)  SCENARIO="$2";  shift 2 ;;
    --config)    CONFIG="$2";    shift 2 ;;
    --seed)      SEED="$2";      shift 2 ;;
    --keep)      KEEP=1;         shift   ;;
    --docker)    DOCKER=1;       shift   ;;
    --out-dir)   OUT_DIR="$2";   shift 2 ;;
    --shard-flag)   SHARD_FLAGS+=("$2"); shift 2 ;;
    --table-suffix) TABLE_SUFFIX="$2";   shift 2 ;;
    --shard-log-level) SHARD_LOG_LEVEL="$2"; shift 2 ;;
    --shards)    SHARDS="$2";    shift 2 ;;
    --tpcds-cache) TPCDS_CACHE="$2"; shift 2 ;;
    --preview-arg) PREVIEW_EXTRA+=("$2"); shift 2 ;;
    -h|--help)
      sed -n '2,47p' "$0"; exit 0 ;;
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
  # <file>` refuses. Prefer the cmd package when there is one, and
  # cmd/connector where cmd holds several mains.
  if [[ -d "$ROOT_DIR/$CONNECTOR/cmd/connector" ]]; then
    BUILD_PKG="./$CONNECTOR/cmd/connector"
  elif [[ -d "$ROOT_DIR/$CONNECTOR/cmd" ]]; then
    BUILD_PKG="./$CONNECTOR/cmd/..."
  else
    BUILD_PKG="./$CONNECTOR/..."
  fi
  echo "building connector binary: $CONNECTOR_BIN (from $BUILD_PKG)"
  (cd "$ROOT_DIR" && go build -v -tags nozstd -o "$CONNECTOR_BIN" "$BUILD_PKG")
  # "go build -o" against a pattern that matches no main package compiles the
  # libraries and exits 0 without writing anything, which surfaces much later
  # as a preview that cannot start the connector.
  [[ -x "$CONNECTOR_BIN" ]] || {
    echo "build produced no binary at $CONNECTOR_BIN ($BUILD_PKG matched no main package)" >&2
    exit 1
  }
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
if (( ${#SHARD_FLAGS[@]} )); then echo "shard flags: ${SHARD_FLAGS[*]}"; fi
if [[ -n "$TABLE_SUFFIX" ]]; then echo "table suffix: $TABLE_SUFFIX"; fi
echo "shards:    $SHARDS"
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

TPCDS=0
if python3 -c 'import sys, yaml; sys.exit("tpcds" not in (yaml.safe_load(open(sys.argv[1])) or {}))' "$SCENARIO"; then
  TPCDS=1
  CAPTURE_SPEC="$OUT_DIR/capture.flow.yaml"
  CAPTURE_TASK="bench/source-tpc-ds${RUN_SUFFIX}"
  echo "workload:  TPC-DS via ${TPCDS_IMAGE:-ghcr.io/estuary/source-tpc-ds:dev}"
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
if (( TPCDS )); then
  SPEC_ARGS+=(--capture-output "$CAPTURE_SPEC" --capture-task "$CAPTURE_TASK")
fi
if (( DOCKER )); then
  SPEC_ARGS+=(--docker)
fi
for flag in "${SHARD_FLAGS[@]+"${SHARD_FLAGS[@]}"}"; do
  SPEC_ARGS+=(--shard-flag "$flag")
done
if [[ -n "$TABLE_SUFFIX" ]]; then
  SPEC_ARGS+=(--table-suffix "$TABLE_SUFFIX")
fi
if [[ -n "$SHARD_LOG_LEVEL" ]]; then
  SPEC_ARGS+=(--shard-log-level "$SHARD_LOG_LEVEL")
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
if (( TPCDS )); then
  TPCDS_ARGS=(
    --scenario "$SCENARIO"
    --source   "$CAPTURE_SPEC"
    --name     "$CAPTURE_TASK"
    --log      "$OUT_DIR/capture.log"
  )
  if [[ -n "$TPCDS_CACHE" ]]; then
    TPCDS_ARGS+=(--cache "$TPCDS_CACHE")
    # Generate before the preview starts, so the run times only the replay.
    python3 "$BENCH_DIR/tpcds.py" prepare "${TPCDS_ARGS[@]}"
    echo "replaying cached TPC-DS fixture from $TPCDS_CACHE"
  else
    echo "starting source-tpc-ds capture (log: $OUT_DIR/capture.log)"
  fi
  python3 "$BENCH_DIR/tpcds.py" relay "${TPCDS_ARGS[@]}" \
    --output "$FIFO" \
    --state  "$STATE_LOG" \
    >"$OUT_DIR/generator.stderr" 2>&1 &
else
  echo "starting generator (seed=$SEED)"
  python3 "$BENCH_DIR/generate.py" \
    --scenario "$SCENARIO" \
    --seed     "$SEED" \
    --output   "$FIFO" \
    --state    "$STATE_LOG" \
    >"$OUT_DIR/generator.stderr" 2>&1 &
fi
GEN_PID=$!

# Run the preview, timing the whole thing.
PREVIEW_LOG="$OUT_DIR/preview.log"

PREVIEW_ARGS=(
  --source  "$SPEC"
  --name    "$TASK"
  --fixture "$FIFO"
  --shards  "$SHARDS"
)
# Docker mode needs --network so the connector container can reach the DB.
if (( DOCKER )); then
  PREVIEW_ARGS+=(--network flow-test)
fi
PREVIEW_ARGS+=("${PREVIEW_EXTRA[@]+"${PREVIEW_EXTRA[@]}"}")

# flowctl publishes one `transaction stats` line per round at info, which is
# what results.py derives the transaction boundaries from. The task keeps its
# default warn level, which still forwards the connector's own warnings as
# `ops:` lines.
export RUST_LOG="info"

echo "running flowctl raw preview-next --shards $SHARDS (logs: $PREVIEW_LOG)"
set +e
flowctl raw preview-next "${PREVIEW_ARGS[@]}" \
  >"$OUT_DIR/preview.stdout" 2>"$PREVIEW_LOG"
PREVIEW_RC=$?
set -e

# If the preview died early, the generator is blocked writing to a FIFO no one
# reads. Kill it so we don't hang. After a clean preview it has closed the FIFO
# and is at most stopping its capture.
if (( PREVIEW_RC )) && kill -0 $GEN_PID 2>/dev/null; then
  kill $GEN_PID 2>/dev/null || true
fi
GEN_RC=0
wait $GEN_PID 2>/dev/null || GEN_RC=$?

echo "preview exit: $PREVIEW_RC"
if (( GEN_RC )); then
  echo "generator exit: $GEN_RC (see $OUT_DIR/generator.stderr)"
  # A preview that drained a truncated fixture still exits 0.
  (( PREVIEW_RC )) || PREVIEW_RC=$GEN_RC
fi

# Build results.json from the state log and the preview log's own timings.
python3 "$BENCH_DIR/results.py" "$STATE_LOG" "$PREVIEW_RC" "$CONNECTOR" "$SCENARIO" "$SEED" "$PREVIEW_LOG" >"$OUT_DIR/results.json"

echo "results: $OUT_DIR/results.json"

# Pretty-print the results table.
python3 "$BENCH_DIR/print_results.py" "$OUT_DIR/results.json" || true

exit $PREVIEW_RC
