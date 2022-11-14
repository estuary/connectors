#!/bin/bash

set -e

command -v flowctl-admin >/dev/null 2>&1 || { echo >&2 "flowctl-admin must be available via PATH, aborting."; exit 1; }

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

# The dir of materialize tests.
TEST_BASE_DIR="${ROOT_DIR}/tests/materialize"

# The dir containing various helper scripts.
TEST_SCRIPT_DIR="${ROOT_DIR}/tests/materialize/flowctl-admin"

# The dir containing test scripts of the connector.
CONNECTOR_TEST_SCRIPTS_DIR="${TEST_BASE_DIR}/${CONNECTOR}"

# Export envvars to be shared by all scripts.

# Build ID to use for tests.
export BUILD_ID=run-test-"${CONNECTOR}"

# A directory for hosting temp files generated during the test executions.
export TEST_DIR="$(mktemp -d /tmp/"${CONNECTOR}"-XXXXXX)"

# `flowctl-admin` commands which interact with the data plane look for *_ADDRESS
# variables, which are created by the temp-data-plane we're about to start.
export BROKER_ADDRESS=unix://localhost${TEST_DIR}/gazette.sock
export CONSUMER_ADDRESS=unix://localhost${TEST_DIR}/consumer.sock

# Used for the ingestion client to build a unix socket dialer.
export INGEST_SOCKET_PATH=${TEST_DIR}/consumer.sock

# The path to the data ingestion go script.
export DATA_INGEST_SCRIPT_PATH="${TEST_BASE_DIR}/datasets/ingest.go"

# The file name of catalog specification.
# The file is located in ${TEST_DIR}
export CATALOG="materialize-test.flow.yaml"

# The name of the test capture for pushing data into collections.
export PUSH_CAPTURE_NAME=test/ingest

# A few envvars related to test data and collections.
# They are used by the connector-specific script of `ingest-data.sh`.
# 1. the names of prepared flow collections.
export TEST_COLLECTION_SIMPLE="tests/${CONNECTOR}/simple"
export TEST_COLLECTION_DUPLICATED_KEYS="tests/${CONNECTOR}/duplicated-keys"
export TEST_COLLECTION_MULTIPLE_DATATYPES="tests/${CONNECTOR}/multiple-datatypes"

# 2. the paths of test datasets that matches the test collections.
# - a dataset that matches the schema of TEST_COLLECTION_SIMPLE.
export DATASET_SIMPLE="${ROOT_DIR}/tests/materialize/datasets/simple.jsonl"
# - a dataset that matches the schema of TEST_COLLECTION_DUPLICATED_KEYS.
export DATASET_DUPLICATED_KEYS="${ROOT_DIR}/tests/materialize/datasets/duplicated-keys.jsonl"
# - a dataset that matches the schema of TEST_COLLECTION_MULTIPLE_DATATYPES.
export DATASET_MULTIPLE_DATATYPES="${ROOT_DIR}/tests/materialize/datasets/multiple-datatypes.jsonl"

# 3. the binding number of datasets in the push capture.
export BINDING_NUM_SIMPLE=0
export BINDING_NUM_DUPLICATED_KEYS=1
export BINDING_NUM_MULTIPLE_DATATYPES=2

# Util function for running the `flowctl-admin combine` to combine the results.
function combineResults() {
    # The collection name specified in the catalog.
    local collection="$1"
    # The input jsonl file contains json docs of the specified collection.
    local input_file_name="$2"
    # The output jsonl file of the combined results.
    local output_file_name="$3"
    source "${TEST_SCRIPT_DIR}/combine.sh" "${collection} ${input_file_name} ${output_file_name}" false || bail "combine results failed."
}
# Export the function to be avialble to connector-specific testing scripts.
export -f combineResults

echo -e "\nexecuting setup"
source "${CONNECTOR_TEST_SCRIPTS_DIR}/setup.sh" || bail "setup failed"

if [[ -z "${CONNECTOR_CONFIG}" ]]; then
    bail "setup did not set CONNECTOR_CONFIG"
fi

if [[ -z "${RESOURCES_CONFIG}" ]]; then
    bail "setup did not set RESOURCES_CONFIG"
fi

echo -e "\ngenerating catalog"
envsubst < ${ROOT_DIR}/tests/materialize/flow.json.template > "${TEST_DIR}/${CATALOG}" \
    || bail "generating ${CATALOG} failed."

echo -e "\nstarting temp data plane"
# Start an empty local data plane within our TESTDIR as a background job.
# --tempdir to use our known TESTDIR rather than creating a new temporary directory.
# --unix-sockets to create UDS socket files in TESTDIR in well-known locations.
flowctl-admin temp-data-plane \
    --log.level info \
    --network "flow-test" \
    --tempdir ${TEST_DIR} \
    --unix-sockets \
    &
DATA_PLANE_PID=$!

# Arrange to stop the data plane on exit.
function cleanup() {
    echo -e "\nexecuting cleanup"

    source "${TEST_SCRIPT_DIR}/delete.sh" || true
    source "${CONNECTOR_TEST_SCRIPTS_DIR}/cleanup.sh" || true
    kill -s SIGTERM ${DATA_PLANE_PID} && wait ${DATA_PLANE_PID} || true
}
trap cleanup EXIT

echo -e "\nbuilding and activating test catalog"
source "${TEST_SCRIPT_DIR}/build-and-activate.sh" false || bail "building and activating test catalog failed."

echo -e "\nexecuting ingest-data"
source "${CONNECTOR_TEST_SCRIPTS_DIR}/ingest-data.sh" || bail "ingest data failed."

# The relative path to ${TEST_DIR} for storing result files.
result_dir=result_jsonl
mkdir -p "$(realpath "${TEST_DIR}"/"${result_dir}")"

echo -e "\nexecuting fetch-materialize-results"
source "${CONNECTOR_TEST_SCRIPTS_DIR}/fetch-materialize-results.sh" ${result_dir}\
    || bail "fetching test results failed."

echo -e "\ncomparing actual v.s. expected"
# The dir that contains all the files expected from the output.
EXPECTED_DIR="${CONNECTOR_TEST_SCRIPTS_DIR}/expected/"

# Make sure all expected files has their corresponding files in the actual output directory.
for f in "${EXPECTED_DIR}"/*; do
    filename="$(basename "${f}")"
    if [[ ! -f "${TEST_DIR}/${result_dir}/${filename}" ]]; then
        bail "expected file ${result_dir}/${filename} is missing in the actual output."
    fi
done

# Make sure all files in the actual output directory are same as expected.
for f in "${TEST_DIR}/${result_dir}"/*; do
    expected="${EXPECTED_DIR}/$(basename "${f}")"
    if [[ ! -f ${expected} ]]; then
        bail "${f} in the actual output is not expected."
    fi

    jq --sort-keys -c <"${f}" >"${f}.normalized" || bail "Failed to normalize ${f}."

    diff --suppress-common-lines --side-by-side "${f}.normalized" "${expected}" \
    || bail "Test Failed because ${expected} is different"
done


echo "Tests Passed!"
