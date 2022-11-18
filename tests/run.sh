#!/bin/bash
#
# This script executes an end-to-end integration test of a flow catalog with a given connector.
# The connector name is read from the CONNECTOR env variable, and the image tag is read from
# VERSION. Both of those are required.
# The tests will execute a connector-specific setup script, then run a flow data plane with a
# generated catalog that uses the connector to capture some data.

# -e causes the script to exit on encountering an error
# -m turns on job management, required for our use of `fg` below.
set -em

command -v flowctl-go >/dev/null 2>&1 || { echo >&2 "flowctl-go must be available via PATH, aborting."; exit 1; }

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
OUTPUT_DB="${TESTDIR}/materialization.db"
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

# `flowctl-go` commands which interact with the data plane look for *_ADDRESS
# variables, which are created by the temp-data-plane we're about to start.
export BROKER_ADDRESS=unix://localhost${TESTDIR}/gazette.sock
export CONSUMER_ADDRESS=unix://localhost${TESTDIR}/consumer.sock

# Start an empty local data plane within our TESTDIR as a background job.
# --poll so that connectors are polled rather than continuously tailed.
# --sigterm to verify we cleanly tear down the test catalog (otherwise it hangs).
# --tempdir to use our known TESTDIR rather than creating a new temporary directory.
# --unix-sockets to create UDS socket files in TESTDIR in well-known locations.
flowctl-go temp-data-plane \
    --log.level info \
    --poll \
    --sigterm \
    --network "flow-test" \
    --tempdir ${TESTDIR} \
    --unix-sockets \
    &
DATA_PLANE_PID=$!
# Arrange to stop the data plane on exit.
trap "kill -s SIGTERM ${DATA_PLANE_PID} && wait ${DATA_PLANE_PID}" EXIT

# Get the spec from the connector and ensure it's valid json.
flowctl-go api spec --image "${CONNECTOR_IMAGE}" | jq -cM || bail "failed to validate spec"

# Execute test-specific setup steps.
echo -e "\nexecuting setup"
source "tests/${CONNECTOR}/setup.sh" || bail "${CONNECTOR}/setup.sh failed"
if [[ -z "$RESOURCE" ]]; then
    bail "setup did not set RESOURCE"
fi

if [[ -z "$CONNECTOR_CONFIG" ]]; then
    bail "setup did not set CONNECTOR_CONFIG"
fi
trap "kill -s SIGTERM ${DATA_PLANE_PID} && wait ${DATA_PLANE_PID} && ./tests/${CONNECTOR}/cleanup.sh" EXIT

# The source-s3 and source-gcs connectors need the collection schema to use a string for the id
# property because the parser treats all CSV columns as strings. Their setup.sh scripts will set
# ID_TYPE to 'string', and the template uses this variable in the schema. We default the variable to
# integer here so that not all setup scripts need to export it.
export ID_TYPE="${ID_TYPE:-integer}"

# Verify discover works
flowctl-go api discover --image="${CONNECTOR_IMAGE}" --network "flow-test" --log.level=debug --config=<(echo ${CONNECTOR_CONFIG}) > ${TESTDIR}/discover_output.json || bail "Discover failed."
cat ${TESTDIR}/discover_output.json | jq ".bindings[] | select(.recommendedName == \"${TEST_STREAM}\") | .documentSchema" > ${TESTDIR}/bindings.json

if [[ -f "tests/${CONNECTOR}/bindings.json" ]]; then
  diff --side-by-side ${TESTDIR}/bindings.json "tests/${CONNECTOR}/bindings.json" || bail "Discovered bindings are wrong"
fi

# Generate the test-specific catalog source.
cat tests/template.flow.yaml | envsubst > "${CATALOG_SOURCE}"

# Build the catalog.
flowctl-go api build --directory ${TESTDIR}/builds --build-id test-build-id --source ${CATALOG_SOURCE} --network "flow-test" --ts-package || bail "Build failed."

# Activate the catalog.
flowctl-go api activate --build-id test-build-id --all --network "flow-test" --log.level info || bail "Activate failed."
# Wait for a data-flow pass to finish.
flowctl-go api await --build-id test-build-id --log.level info || bail "Await failed."
# Read out materialization results.
sqlite3 -header "${OUTPUT_DB}" "select id, canary from test_results;" > "${ACTUAL}"
# Clean up the activated catalog.
flowctl-go api delete --build-id test-build-id --all --log.level info || bail "Delete failed."

# Verify actual vs expected results. `diff` will exit 1 if files are different
diff --suppress-common-lines --side-by-side "${ACTUAL}" "tests/${CONNECTOR}/expected.txt" || bail "Test Failed"

echo "Test Passed"
