#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

command -v flowctl-go >/dev/null 2>&1 || { echo >&2 "flowctl-go must be available via PATH, aborting."; exit 1; }

ROOT_DIR="$(git rev-parse --show-toplevel)"
cd "$ROOT_DIR"

# The docker image of the connector to be tested.
# The connector image needs to be available to envsubst.
export CONNECTOR_IMAGE="ghcr.io/estuary/${CONNECTOR}:${VERSION}"

function bail() {
    >&2 echo "$@"
    exit 1
}
export -f bail

test -n "${CONNECTOR}" || bail "must specify CONNECTOR env variable"
test -n "${VERSION}" || bail "must specify VERSION env variable"

# The base test directory containing test scripts of the connector.
TEST_DIR="${ROOT_DIR}/tests/materialize"
# Temporary directory into which we'll write files.
TEMP_DIR=$(mktemp -d /tmp/${CONNECTOR}-XXXXXX)

# Arrange to clean up on exit.
TEST_STATUS="Test Failed"
function test_shutdown() {
    source ${TEST_DIR}/${CONNECTOR}/cleanup.sh || true
    rm -r ${TEMP_DIR}
    >&2 echo -e "===========\n${TEST_STATUS}\n==========="
}
trap test_shutdown EXIT


# TODO(johnny): These no longer need to be enviornment variables.
# I'm leaving them as such to avoid churning tests more than is needed.
export TEST_COLLECTION_SIMPLE="tests/simple"
export TEST_COLLECTION_DUPLICATED_KEYS="tests/duplicated-keys"
export TEST_COLLECTION_MULTIPLE_DATATYPES="tests/multiple-data-types"
export TEST_COLLECTION_FORMATTED_STRINGS="tests/formatted-strings"

source ${TEST_DIR}/${CONNECTOR}/setup.sh || bail "setup failed"

if [[ -z "${CONNECTOR_CONFIG}" ]]; then
    bail "setup did not set CONNECTOR_CONFIG"
fi
if [[ -z "${RESOURCES_CONFIG}" ]]; then
    bail "setup did not set RESOURCES_CONFIG"
fi

envsubst < ${TEST_DIR}/flow.json.template > ${TEMP_DIR}/flow.json \
    || bail "generating catalog failed"
envsubst < ${TEST_DIR}/empty.flow.json.template > ${TEMP_DIR}/empty.flow.json \
    || bail "generating empty catalog failed"

# File into which we'll write a snapshot of the connector output.
SNAPSHOT=${TEST_DIR}/${CONNECTOR}/snapshot.json
rm ${SNAPSHOT} || true

function drive_connector {
    local source=$1;
    local fixture=$2;

    # Build source into a build DB. We must go through flowctl-go because flowctl
    # hasn't implemented validation of connector images yet.
    flowctl-go api build \
        --build-id build-id \
        --build-db ${TEMP_DIR}/build.db \
        --network flow-test \
        --source ${source} \
        --log.level info \
        || bail "building catalog failed"

    # Pluck out the protobuf-encoded materialization.
    sqlite3 ${TEMP_DIR}/build.db "select writefile('${TEMP_DIR}/spec.proto', spec) from built_materializations;"

    # Drive the connector, writing its output into the snapshot.
    # The first jq command orders all object keys and sanitizes base64-encoded
    # checkpoints and specifications, having one line per message output.
    # We sort all Response.Loaded responses in ascending lexicographic order,
    # and then snapshot the pretty-printed result. This allows connectors to
    # output loads in any overall order and with any inter-document structure,
    # while still having a stable snapshot result.
    ~/flowctl raw materialize-fixture --source ${TEMP_DIR}/spec.proto --fixture ${fixture} | \
        docker run --rm -i -e FLOW_RUNTIME_CODEC=json --network flow-test ${CONNECTOR_IMAGE} | \
        jq -Sc 'if .applied.actionDescription != null then .applied.actionDescription |= sub("[A-Za-z0-9+/=]{100,}"; "(a-base64-encoded-value)") else . end' | \
        go run ${TEST_DIR}/sort-loaded.go | \
        jq '.' >> ${SNAPSHOT} \
        || bail "connector invocation failed"
}

# Drive the connector with the fixture.
drive_connector ${TEMP_DIR}/flow.json ${TEST_DIR}/fixture.yaml

# Extend the snapshot with additional fetched content for this connector.
source ${TEST_DIR}/${CONNECTOR}/fetch.sh | jq -S '.' >> ${SNAPSHOT} || bail "fetching results failed"

# Drive it again to excercise any cleanup behavior when bindings are removed.
drive_connector ${TEMP_DIR}/empty.flow.json ${TEST_DIR}/empty.fixture.yaml

# Compare actual and expected snapshots.
# Produce patch output, so that differences in CI can be reviewed and manually
# patched into the expectation without having to run the test locally via:
git diff --exit-code ${SNAPSHOT} || bail "Test Failed because snapshot is different"

TEST_STATUS="Test Passed"




