#!/bin/bash
#
# Run the TPC-DS baselines: one run.sh per tabular materialization, serially,
# replaying one cached fixture, and copy each completed run's results.json to
# runs/baselines/<connector>/<scenario>.json with the environment it ran in.
#
# Usage:
#   tests/benchmark/materialize/baselines.sh --tpcds-cache DIR [ENTRY ...]
#
# An ENTRY is connector:config[:scenario] (scenario defaults to tpcds-scale-1);
# without entries every connector below runs. flowctl on PATH should be built
# from Flow master. FLOW_COMMIT and MACHINE_TYPE, when set, are recorded too.

set -o nounset

ROOT_DIR="$(git rev-parse --show-toplevel)"
B="tests/benchmark/materialize"
cd "$ROOT_DIR"

CACHE=""
while (($#)); do
  case "$1" in
    --tpcds-cache) CACHE="$2"; shift 2 ;;
    -h|--help) sed -n '2,13p' "$0"; exit 0 ;;
    *) break ;;
  esac
done
[[ -n "$CACHE" ]] || { echo "--tpcds-cache is required" >&2; exit 2; }

# Quickest first, so a slow destination can't hold up the rest.
RUNS=(
  materialize-postgres:materialize-postgres/testdata/config.local.yaml
  materialize-mysql:materialize-mysql/testdata/config.mysql.yaml
  materialize-sqlserver:materialize-sqlserver/testdata/config.local.yaml
  materialize-clickhouse:materialize-clickhouse/testdata/config.local.yaml
  materialize-s3-parquet:materialize-s3-parquet/testdata/config.local.yaml
  materialize-snowflake:materialize-snowflake/testdata/config.yaml
  materialize-databricks:materialize-databricks/testdata/config.local.yaml
  materialize-bigquery:materialize-bigquery/testdata/config.yaml
  materialize-redshift:materialize-redshift/testdata/config.local.yaml
  materialize-azure-fabric-warehouse:materialize-azure-fabric-warehouse/testdata/config.yaml
  materialize-motherduck:materialize-motherduck/testdata/config.s3.yaml
  materialize-iceberg:materialize-iceberg/testdata/config.rest-local.yaml
  materialize-s3-iceberg:materialize-s3-iceberg/testdata/config.yaml
  materialize-mongodb:materialize-mongodb/testdata/config.local.yaml
  materialize-elasticsearch:materialize-elasticsearch/testdata/config.local.yaml
  materialize-spanner:materialize-spanner/testdata/config.gcp.yaml
)
(( $# )) && RUNS=("$@")

CFG=$(mktemp -d)
trap 'rm -rf "$CFG"' EXIT

MACHINE_TYPE=${MACHINE_TYPE:-$(curl -sf -H "Metadata-Flavor: Google" \
  http://metadata.google.internal/computeMetadata/v1/instance/machine-type 2>/dev/null | sed 's#.*/##')}
CONNECTORS_COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "${CONNECTORS_COMMIT:-}")
export MACHINE_TYPE CONNECTORS_COMMIT FLOW_COMMIT=${FLOW_COMMIT:-}

# Edit a config copy, sops-encrypted or not, by a Python function of its dict.
edit_config() {
  local path=$1 script=$2
  if grep -q '^sops:' "$path"; then
    # The decrypted config travels on stdin: a command line is readable by
    # every local user through /proc/<pid>/cmdline.
    sops -d "$path" | python3 /dev/fd/3 "$path" "$script" 3<<'PY'
import json, subprocess, sys, yaml
path, script = sys.argv[1], sys.argv[2]
c = yaml.safe_load(sys.stdin)
exec(script)
for key in ("advanced", "schema"):
    if key in c:
        subprocess.run(["sops", "set", path, f'["{key}"]', json.dumps(c[key])], check=True)
PY
  else
    python3 - "$path" "$script" <<'PY'
import sys, yaml
path, script = sys.argv[1], sys.argv[2]
c = yaml.safe_load(open(path))
exec(script)
yaml.safe_dump(c, open(path, "w"), sort_keys=False)
PY
  fi
}

# Copy a config for benchmarking. Integration-test configs set flags which make
# the runtime Load every key; drop them. Fabric detects snapshot conflicts per
# table, so a dedicated schema keeps other tasks' commits to the shared
# flow_checkpoints_v1 from aborting the run's.
prepare_config() {
  local c=$1 src=$2 dst=$3
  cp "$src" "$dst"
  edit_config "$dst" '
adv = c.get("advanced") or {}
flags = [f for f in (adv.get("feature_flags") or "").split(",")
         if f and f not in ("allow_existing_tables_for_new_bindings", "retain_existing_data_on_backfill")]
if "feature_flags" in adv:
    adv["feature_flags"] = ",".join(flags)
'
  if [[ "$c" == materialize-azure-fabric-warehouse ]]; then
    edit_config "$dst" 'c["schema"] = "flow_bench_tpcds"'
  fi
}

flowctl --version
for run in "${RUNS[@]}"; do
  IFS=: read -r c src scenario <<<"$run"
  scenario=${scenario:-tpcds-scale-1}
  out=$B/runs/baseline-$scenario-$c
  echo "=== $c start $(date -u +%FT%TZ)"
  cfg=$CFG/$c.$(basename "$src")
  prepare_config "$c" "$src" "$cfg" || { echo "=== $c SKIP config"; continue; }
  rm -rf "$out"
  timeout -s TERM 3h ./$B/run.sh --connector "$c" --config "$cfg" \
    --scenario "$B/scenarios/$scenario.yaml" \
    --tpcds-cache "$CACHE" --out-dir "$out" > "$out.log" 2>&1
  rc=$?
  echo "=== $c rc=$rc end $(date -u +%FT%TZ) $(grep -E 'Docs/sec' "$out.log" | tr -s ' ')"
  if [[ $rc -eq 0 && -f "$out/results.json" ]]; then
    mkdir -p "$B/runs/baselines/$c"
    python3 - "$out/results.json" "$B/runs/baselines/$c/$scenario.json" "$(flowctl --version)" <<'PY'
import json, os, sys
src, dst, flowctl = sys.argv[1:4]
r = json.load(open(src))
r["environment"] = {
    "machine": os.environ.get("MACHINE_TYPE") or None,
    "flowctl": flowctl,
    "flow_commit": os.environ.get("FLOW_COMMIT") or None,
    "connectors_commit": os.environ.get("CONNECTORS_COMMIT") or None,
}
json.dump(r, open(dst, "w"), indent=2)
PY
  fi
done
echo "=== all done"
