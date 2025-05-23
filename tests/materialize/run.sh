#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

command -v flowctl >/dev/null 2>&1 || { echo >&2 "flowctl must be available via PATH, aborting."; exit 1; }

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
export TEST_COLLECTION_LONG_STRING="tests/long-string"
export TEST_COLLECTION_COMPOUND_KEY="tests/compound-key"
export TEST_COLLECTION_SYMBOLS="tests/symbols"
export TEST_COLLECTION_UNSIGNED_BIGINT="tests/unsigned-bigint"
export TEST_COLLECTION_DELETIONS="tests/deletions"
export TEST_COLLECTION_BINARY_KEY="tests/binary-key"
export TEST_COLLECTION_STRING_ESCAPED_KEY="tests/string-escaped-key"
export TEST_COLLECTION_UNDERSCORE_COLUMN="tests/underscore-column"

function decrypt_config {
  sops --output-type json --decrypt $1 | jq 'walk( if type == "object" then with_entries(.key |= rtrimstr("_sops")) else . end)' 
}

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

    # Drive the connector, writing its output into the snapshot.
    RUST_LOG=info flowctl preview --source ${source} --fixture ${fixture} --output-state --output-apply --network flow-test \
      | jq '.' >> ${SNAPSHOT} \
      || bail "connector invocation failed"
}

# Drive the connector with the fixture.
drive_connector ${TEMP_DIR}/flow.json ${TEST_DIR}/fixture.json

# Extend the snapshot with additional fetched content for this connector.
source ${TEST_DIR}/${CONNECTOR}/fetch.sh | jq -S '.' >> ${SNAPSHOT} || bail "fetching results failed"

# Drive it again to excercise any cleanup behavior when bindings are removed.
drive_connector ${TEMP_DIR}/empty.flow.json ${TEST_DIR}/empty.fixture.json

if [[ -f "${TEST_DIR}/${CONNECTOR}/checks.sh" ]]; then
  # Run connector-specific checks
  source ${TEST_DIR}/${CONNECTOR}/checks.sh || bail "connector-specific checks failed"
fi

# Compare actual and expected snapshots.
# Produce patch output, so that differences in CI can be reviewed and manually
# patched into the expectation without having to run the test locally via:
git diff --exit-code ${SNAPSHOT} || bail "Test Failed because snapshot is different"

TEST_STATUS="Test Passed"




