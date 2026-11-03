#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

command -v flowctl >/dev/null 2>&1 || { echo >&2 "flowctl must be available via PATH, aborting."; exit 1; }
test -n "${CONNECTOR}" || bail "must specify CONNECTOR env variable"
test -n "${VERSION}" || bail "must specify VERSION env variable"

# Runs `go test ./...` once per filter, concurrently, prints each run's output
# once all have exited, and fails if any of them failed.
function go_test_concurrently() {
  local timeout=$1
  shift
  local logs pids=() failed=0 i
  logs="$(mktemp -d)"
  # Compile once up front rather than in every run at the same time.
  go test -run '^$' ./... >/dev/null
  for i in $(seq $#); do
    # The filter is unquoted so that it splits into its flag and pattern.
    go test -v ./... -timeout "$timeout" ${!i} >"$logs/$i.log" 2>&1 &
    pids+=($!)
  done
  for i in $(seq $#); do
    if wait "${pids[$((i-1))]}"; then
      echo "::group::go test ${!i} (ok)"
      cat "$logs/$i.log"
      echo "::endgroup::"
    else
      failed=1
      echo "go test ${!i} FAILED:"
      cat "$logs/$i.log"
    fi
  done
  return $failed
}

ROOT_DIR="$(git rev-parse --show-toplevel)"
cd "$ROOT_DIR"

# Run the specific connector tests.
cd $ROOT_DIR/$CONNECTOR
if [ -f Cargo.toml ]; then
  cargo test
else
  # Test common materialization packages. This is repeated for each
  # materialization during CI runs which is quite redundant, but these tests are
  # fast so it doesn't really matter. This should be optimized later with other CI
  # improvements.

  cd $ROOT_DIR/go && go test -v ./...
  cd $ROOT_DIR/filesink && go test -v ./...
  cd $ROOT_DIR/materialize-boilerplate && go test -v ./...

  TIMEOUT=20m
  if [ "$CONNECTOR" = "materialize-spanner" ]; then
    TIMEOUT=40m
  elif [ "$CONNECTOR" = "materialize-iceberg" ]; then
    TIMEOUT=60m
  elif [ "$CONNECTOR" = "materialize-snowflake" ]; then
    TIMEOUT=30m
  fi
  cd $ROOT_DIR/$CONNECTOR
  if [ "$CONNECTOR" = "materialize-snowflake" ]; then
    # Each group spends most of its time waiting on Snowflake. They run as
    # separate processes because the streaming tests repin package state.
    go_test_concurrently $TIMEOUT \
      '-run ^TestIntegration$' \
      '-run ^TestStreamV2Manager$' \
      '-skip ^(TestIntegration|TestStreamV2Manager)$'
  else
    go test -v ./... -timeout $TIMEOUT
  fi
fi

# Verify that the built image at least works enough to run the spec command.
# This is intended as a smoke test to detect completely broken images, since the
# connector integration tests run with a local command, rather than a built
# container image.
echo "{\"materializations\":{\"acmeCo/tests/spec\":{\"endpoint\":{\"connector\":{\"image\":\"ghcr.io/estuary/${CONNECTOR}:${VERSION}\",\"config\":{}}},\"bindings\":[]}}}" | flowctl raw spec --source - | jq >/dev/null 2>&1
