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

flowctl --version
for run in "${RUNS[@]}"; do
  IFS=: read -r c src scenario <<<"$run"
  scenario=${scenario:-tpcds-scale-1}
  out=$B/runs/baseline-$scenario-$c
  echo "=== $c start $(date -u +%FT%TZ)"
  cfg=$CFG/$c.$(basename "$src")
  python3 "$B/baselines.py" prepare-config "$c" "$src" "$cfg" || { echo "=== $c SKIP config"; continue; }
  rm -rf "$out"
  timeout -s TERM 3h ./$B/run.sh --connector "$c" --config "$cfg" \
    --scenario "$B/scenarios/$scenario.yaml" \
    --tpcds-cache "$CACHE" --out-dir "$out" > "$out.log" 2>&1
  rc=$?
  echo "=== $c rc=$rc end $(date -u +%FT%TZ) $(grep -E 'Docs/sec' "$out.log" | tr -s ' ')"
  if [[ $rc -eq 0 && -f "$out/results.json" ]]; then
    python3 "$B/baselines.py" record "$out/results.json" "$B/runs/baselines/$c/$scenario.json"
  fi
done
echo "=== all done"
