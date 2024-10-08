#!/bin/bash
#
# This script executes an end-to-end integration test of a flow catalog with a given connector.
# The connector name is read from the CONNECTOR env variable, and the image tag is read from
# VERSION. Both of those are required.
# The tests will execute a connector-specific setup script, then run a flow data plane with a
# generated catalog that uses the connector to capture some data.

# -m turns on job management, required for our use of `fg` below.
set -m
set -o errexit
set -o pipefail
set -o nounset

command -v flowctl-go >/dev/null 2>&1 || {
  echo >&2 "flowctl-go must be available via PATH, aborting."
  exit 1
}

ROOT_DIR="$(git rev-parse --show-toplevel)"
cd "$ROOT_DIR"

function bail() {
  echo "$@" 1>&2
  exit 1
}

test -n "$CONNECTOR" || bail "must specify CONNECTOR env variable"
test -n "$VERSION" || bail "must specify VERSION env variable"

# Connector image to use. Export to make it available to `envsubst`
export CONNECTOR_IMAGE="ghcr.io/estuary/${CONNECTOR}:${VERSION}"
echo "testing connector: '$CONNECTOR'"

# Directory under which the test runs.
TESTDIR=".build/tests/${CONNECTOR}"
# Post-templating catalog source processed by the test.
CATALOG_SOURCE="${TESTDIR}/test.flow.yaml"
# SQLite database into which the test is expected to materialize.
OUTPUT_DB="/tmp/sqlite.db"
# Actual materialization output scraped from ${OUTPUT_DB}.
ACTUAL="${TESTDIR}/actual_test_results.txt"

# Ensure we start with an empty dir, since temporary data plane files will go here.
# Remove it, if it exists already.
if [[ -d "${TESTDIR}" ]]; then
  rm -r ${TESTDIR}
fi
mkdir -p "${TESTDIR}"

# Map to an absolute directory.
export TESTDIR=$(realpath ${TESTDIR})

export TMPDIR="/tmp/test"
if [[ -d "${TMPDIR}" ]]; then
  rm -r ${TMPDIR}
fi
mkdir -p "${TMPDIR}"

# `flowctl-go` commands which interact with the data plane look for *_ADDRESS
# variables, which are created by the temp-data-plane we're about to start.
export BROKER_ADDRESS=unix://localhost${TMPDIR}/gazette.sock
export CONSUMER_ADDRESS=unix://localhost${TMPDIR}/consumer.sock

# Start an empty local data plane within our TESTDIR as a background job.
# --sigterm to verify we cleanly tear down the test catalog (otherwise it hangs).
# --tempdir to use our known TESTDIR rather than creating a new temporary directory.
# --unix-sockets to create UDS socket files in TESTDIR in well-known locations.
flowctl-go temp-data-plane \
  --log.level info \
  --sigterm \
  --network "flow-test" \
  --tempdir ${TMPDIR} \
  --unix-sockets \
  &
DATA_PLANE_PID=$!
# Arrange to stop the data plane on exit.
trap "kill -s SIGTERM ${DATA_PLANE_PID} && wait ${DATA_PLANE_PID}" EXIT

# Execute test-specific setup steps.
echo -e "\nexecuting setup"
source "tests/${CONNECTOR}/setup.sh" || bail "${CONNECTOR}/setup.sh failed"
if [[ -z "$RESOURCE" ]]; then
  bail "setup did not set RESOURCE"
fi

if [[ -z "$CONNECTOR_CONFIG" ]]; then
  bail "setup did not set CONNECTOR_CONFIG"
fi

# Get the spec from the connector and ensure it's valid json.
cat >"$TESTDIR/spec.yaml" <<EOF
captures:
  tests/${CONNECTOR}/from-source:
    endpoint:
      connector:
        image: "${CONNECTOR_IMAGE}"
        config: ${CONNECTOR_CONFIG}
    bindings: []
EOF

flowctl raw spec --source "$TESTDIR/spec.yaml" | jq -cM || bail "failed to validate spec"

TEST_STATUS="Test Failed"
function test_shutdown() {
  kill -s SIGTERM ${DATA_PLANE_PID} && wait ${DATA_PLANE_PID} && ./tests/${CONNECTOR}/cleanup.sh
  echo -e "===========\n${TEST_STATUS}\n==========="
}
trap "test_shutdown" EXIT

# The source-s3 and source-gcs connectors need the collection schema to use a string for the id
# property because the parser treats all CSV columns as strings. Their setup.sh scripts will set
# ID_TYPE to 'string', and the template uses this variable in the schema. We default the variable to
# integer here so that not all setup scripts need to export it.
export ID_TYPE="${ID_TYPE:-integer}"

# Verify discover works
flowctl raw discover --network flow-test --source $TESTDIR/spec.yaml -o json --emit-raw >${TESTDIR}/discover_output.json || bail "Discover failed."
cat ${TESTDIR}/discover_output.json | jq "select(.recommendedName == \"${TEST_STREAM}\") | .documentSchema" >${TESTDIR}/bindings.json

if [[ -f "tests/${CONNECTOR}/bindings.json" ]]; then
  cat ${TESTDIR}/bindings.json
  cat tests/${CONNECTOR}/bindings.json
  diff --side-by-side ${TESTDIR}/bindings.json "tests/${CONNECTOR}/bindings.json" || bail "Discovered bindings are wrong"
fi

# Generate the test-specific catalog source.
cat tests/template.flow.yaml | envsubst >"${CATALOG_SOURCE}"

BUILD_ID=1122334455667788

# Build the catalog.
flowctl-go api build \
  --build-id ${BUILD_ID} \
  --build-db ${TMPDIR}/builds/${BUILD_ID} \
  --source ${CATALOG_SOURCE} \
  --network "flow-test" ||
  bail "Build failed."

# Activate the catalog.
flowctl-go api activate --build-id ${BUILD_ID} --all --network "flow-test" --log.level info || bail "Activate failed."

# When we try to read the container name below, we assume that there will only be 1
# materialize-sqlite container. This sleep makes it more likely that any materialize-sqlite
# containers started by `api activate` will get cleaned up before our loop.
sleep 15

# Periodically check expected vs actual lines of output, once we reach the same
# number of lines, we stop the plane and then compare the output
retry_counter=0
while true; do
  # Read out materialization results.
  container_id=$(docker ps | grep materialize-sqlite | awk '{ print $1 }')
  docker exec $container_id sqlite3 -header "${OUTPUT_DB}" "select id, canary from test_results order by id, canary;" >"${ACTUAL}"
  if [[ "$(cat tests/${CONNECTOR}/expected.txt | wc -l)" -eq "$(cat ${ACTUAL} | wc -l)" ]]; then
    # Verify actual vs expected results. `diff` will exit 1 if files are different
    echo "-- RUNNING DIFF"
    diff --suppress-common-lines --side-by-side "${ACTUAL}" "tests/${CONNECTOR}/expected.txt" || bail "Test Failed"
    docker stop $container_id
    break
  fi
  # after 30 retries (30 seconds) we timeout
  retry_counter=$((retry_counter + 1))
  if [[ "$retry_counter" -eq "30" ]]; then
    docker stop $container_id
    bail "Timeout reached while checking for expected output"
  fi
  sleep 1
done

# Clean up the activated catalog.
flowctl-go api delete --build-id ${BUILD_ID} --all --log.level info || bail "Delete failed."

# Will be printed by the shutdown trap *after* any shutdown logging from flowctl
TEST_STATUS="Test Passed"
