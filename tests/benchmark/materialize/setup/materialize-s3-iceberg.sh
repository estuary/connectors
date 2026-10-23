#!/usr/bin/env bash
#
# Benchmark setup for materialize-s3-iceberg against its local compose stack:
# create the rustfs bucket, a Polaris catalog over it with the bootstrap
# principal granted catalog_admin, and the namespace, as driver_test.go does.
# Reads the bucket, endpoints and catalog from $BENCH_CONFIG.

set -o errexit
set -o pipefail
set -o nounset

[[ -n "${BENCH_CONFIG:-}" && -f "$BENCH_CONFIG" ]] || {
  echo "BENCH_CONFIG must point to the connector config YAML" >&2
  exit 1
}

# The decrypted config travels on stdin: a command line is readable by every
# local user through /proc/<pid>/cmdline.
{ sops -d "$BENCH_CONFIG" 2>/dev/null || cat "$BENCH_CONFIG"; } |
  python3 "$(dirname "$0")/materialize-s3-iceberg.py"
