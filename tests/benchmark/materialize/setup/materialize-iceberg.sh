#!/usr/bin/env bash
#
# Benchmark setup for materialize-iceberg against its local compose stack:
# Polaris bootstraps fresh client credentials at startup, so fill them into the
# config passed as $BENCH_CONFIG in place of CREDENTIAL_PLACEHOLDER, as
# driver_test.go does for testdata/.local.

set -o errexit
set -o pipefail
set -o nounset

[[ -n "${BENCH_CONFIG:-}" && -f "$BENCH_CONFIG" ]] || {
  echo "BENCH_CONFIG must point to the connector config YAML" >&2
  exit 1
}

CREDS="$ROOT_DIR/materialize-iceberg/testdata/.local/polaris-creds.json"
for _ in $(seq 120); do
  [[ -s "$CREDS" ]] && break
  sleep 0.5
done
[[ -s "$CREDS" ]] || { echo "Polaris did not write $CREDS" >&2; exit 1; }

python3 "$(dirname "$0")/materialize-iceberg.py" "$CREDS" "$BENCH_CONFIG"
