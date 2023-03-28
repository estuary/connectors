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
    echo "$@" 1>&2
    echo "test failed..."
    exit 1
}
export -f bail

test -n "${CONNECTOR}" || bail "must specify CONNECTOR env variable"
test -n "${VERSION}" || bail "must specify VERSION env variable"

# The base test directory containing test scripts of the connector.
TEST_DIR="${ROOT_DIR}/tests/materialize-wip"
# Temporary directory into which we'll write files.
TEMP_DIR=$(mktemp -d /tmp/${CONNECTOR}-XXXXXX)

# Arrange to clean up on exit.
TEST_STATUS="Test Failed"
function test_shutdown() {
    source ${TEST_DIR}/${CONNECTOR}/cleanup.sh || true
    rm -r ${TEMP_DIR}
    echo -e "===========\n${TEST_STATUS}\n==========="
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

# File into which we'll write a snapshot of the connector output.
SNAPSHOT_ACTUAL=${TEST_DIR}/${CONNECTOR}/snapshot.actual
rm ${SNAPSHOT_ACTUAL} || true

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
    ~/flowctl raw materialize-fixture --source ${TEMP_DIR}/spec.proto --fixture ${fixture} | \
        docker run --rm -i -e FLOW_RUNTIME_CODEC=json --network flow-test ${CONNECTOR_IMAGE} | \
        jq 'if .applied.actionDescription != null then .applied.actionDescription |= sub("[A-Za-z0-9+/=]{100,}"; "(a-base64-encoded-value)") else . end' >> ${SNAPSHOT_ACTUAL} \
        || bail "connector invocation failed"
}

# Drive the connector with the fixture.
drive_connector ${TEMP_DIR}/flow.json ${TEST_DIR}/fixture.yaml

# Extend the snapshot with additional fetched content for this connector.
source ${TEST_DIR}/${CONNECTOR}/fetch.sh >> ${SNAPSHOT_ACTUAL} || bail "fetching results failed"

# Compare actual and expected snapshots.
diff \
    --suppress-common-lines \
    --side-by-side \
    ${SNAPSHOT_ACTUAL} ${TEST_DIR}/${CONNECTOR}/snapshot.expected \
    || bail "Test Failed because snapshot is different"

# Only remove the actual snapshot if we passed.
rm ${SNAPSHOT_ACTUAL}

TEST_STATUS="Test Passed"




