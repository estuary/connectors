#!/bin/bash

set -e

ROOT_DIR="$(git rev-parse --show-toplevel)"
cd "$ROOT_DIR"

# Flow image / container name used in testing.
FLOW_IMAGE="ghcr.io/estuary/flow:v0.1.0-464-gfe2bfe4"
FLOW_CONTAINER_NAME="flow-test"

function bail() {
    echo "$@" 1>&2
    # Output logs for debugging if the execution fails for any reason.
    docker logs ${FLOW_CONTAINER_NAME} || true
    exit 1
}
export -f bail

test -n "${CONNECTOR}" || bail "must specify CONNECTOR env variable"
test -n "${VERSION}" || bail "must specify VERSION env variable"

# The dir containing test scripts of the connector.
CONNECTOR_TEST_SCRIPTS_DIR="tests/materialize/${CONNECTOR}"

# The connector image needs to be available to envsubst
export CONNECTOR_IMAGE="ghcr.io/estuary/${CONNECTOR}:${VERSION}"

# Prepare test directory.
TEST_DIR=".build/tests/${CONNECTOR}"
# Ensure we start with an empty dir, since the flowctl_develop directory will go here.
# Unfortunately, we can't just delete it, since the flowctl container is running as root, and so
# all the files it creates will be owned by root. This isn't an issue in CI, since this directory
# won't exist as long as we do a clean checkout, but for local runs, I'd rather error out than to
# run 'sudo rm'.
if [[ -d "${TEST_DIR}" ]]; then
    bail "The test directory '${TEST_DIR}' must not already exist prior to a test run"
fi
mkdir -p "${TEST_DIR}"

# The path of catalog specification.
CATALOG="${TEST_DIR}/materialize-test.flow.yaml"



# Export a few envvars to be shared with the testing scripts with connector-specific logic.
# 1. the names of prepared flow collections.
export TEST_COLLECTION_SIMPLE="tests/${CONNECTOR}/simple"
export TEST_COLLECTION_MULTIPLE_DATATYPES="tests/${CONNECTOR}/multiple-datatypes"

# 2. the local paths of test datasets that matches the test collections.
# - a dataset that matches the schema of TEST_COLLECTION_SIMPLE.
export DATASET_SIMPLE="tests/materialize/datasets/simple.jsonl"
# - a dataset that matches the schema of TEST_COLLECTION_MULTIPLE_DATATYPES.
export DATASET_MULTIPLE_DATATYPES="tests/materialize/datasets/multiple-datatypes.jsonl"

# 3. websocket URLs for ingesting data.
WEBSOCKET_URL_PREFIX=ws://localhost:8080/ingest
# URL for ingesting data to TEST_COLLECTION_SIMPLE.
export WEBSOCKET_URL_SIMPLE=${WEBSOCKET_URL_PREFIX}/${TEST_COLLECTION_SIMPLE}
# URL for ingesting data to TEST_COLLECTION_MULTIPLE_DATATYPES.
export WEBSOCKET_URL_MULTIPLE_DATATYPES=${WEBSOCKET_URL_PREFIX}/${TEST_COLLECTION_MULTIPLE_DATATYPES}

# Start the catalog for testing.
function startFlow() {
    # Run as root, not the flow user, since the user within the container needs to access the
    # docker socket.
    docker run  -d --rm -it \
        --name "${FLOW_CONTAINER_NAME}" \
        --user 0  \
        --mount type=bind,source=/tmp,target=/tmp \
        --mount type=bind,source=/var/run/docker.sock,target=/var/run/docker.sock \
        --mount type=bind,source="$(pwd)",target=/home/flow/project \
        --network=host \
        "${FLOW_IMAGE}" \
        flowctl develop --source "${CATALOG}" --directory "${TEST_DIR}" \
        || bail "flowctl develop failed"
    for i in {1..20}; do
        # Wait until the server is ready for serving.
        # TODO (jixiang) consider checking a status endpoint?
        if docker logs ${FLOW_CONTAINER_NAME} | grep -q "Listening at: "; then
            echo "server starts successfully."
            return
        fi
        echo "Not ready, retrying ${i}."
        sleep 3
    done

    bail "flowctl develop failed to start the server after 60s."
}

# Util function fur running the `flowctl combine` to combine the results.
function combineResults() {
    # The name collection name specified in the catalog.
    local collection="$1"
    local input_jsonl_path="$2"
    local output_jsonl_path="$3"

    docker run -i \
        --user 0 \
        --mount type=bind,source="$(pwd)",target=/home/flow/project \
        --mount type=bind,source=/var/run/docker.sock,target=/var/run/docker.sock \
        "${FLOW_IMAGE}" \
        flowctl combine --source "${CATALOG}" --directory "${TEST_DIR}" --collection "${collection}" \
        <"${input_jsonl_path}" >"${output_jsonl_path}" \
       || bail "flowctl combine failed"
}
# Export the function to be avialble to connector testing scripts.
export -f combineResults

function cleanup() {
    echo -e "\nexecuting cleanup"
    source "${CONNECTOR_TEST_SCRIPTS_DIR}/cleanup.sh" || true
    docker rm -f ${FLOW_CONTAINER_NAME} || true
}
trap cleanup EXIT

echo -e "\nexecuting setup"
source "${CONNECTOR_TEST_SCRIPTS_DIR}/setup.sh" || bail "setup failed"

if [[ -z "${CONNECTOR_CONFIG}" ]]; then
    bail "setup did not set CONNECTOR_CONFIG"
fi

if [[ -z "${RESOURCES_CONFIG}" ]]; then
    bail "setup did not set RESOURCES_CONFIG"
fi

echo -e "\ngenerating catalog"
envsubst < tests/materialize/flow.json.template | yq eval -P | \
sed "s/CONNECTOR_CONFIG_PLACEHOLDER/${CONNECTOR_CONFIG}/g" \
> "${CATALOG}" || bail "generating ${CATALOG} failed."

echo -e "\nstart testing flow"
startFlow

echo -e "\nexecuting ingest-data"
source "${CONNECTOR_TEST_SCRIPTS_DIR}/ingest-data.sh" || bail "ingest data failed."

echo -e "\nexecuting fetch-materialize-results"
TMP_DIR="$(realpath "${TEST_DIR}/result_data")"
TEST_OUTPUT_JSONL_DIR="$(realpath "${TEST_DIR}/result_jsonl")"
mkdir -p "${TMP_DIR}" "${TEST_OUTPUT_JSONL_DIR}"
source "${CONNECTOR_TEST_SCRIPTS_DIR}/fetch-materialize-results.sh" "${TMP_DIR}" "${TEST_OUTPUT_JSONL_DIR}" \
    || bail "fetching test results failed."


echo -e "\ncomparing acutal v.s. expected"
# The dir that contains all the files expected from the output.
EXPECTED_DIR="${CONNECTOR_TEST_SCRIPTS_DIR}/expected/"

# Make sure all expected files has their corresponding files in the actual output directory.
for f in "${EXPECTED_DIR}"/*; do
    filename="$(basename "${f}")"
    if [[ ! -f "${TEST_OUTPUT_JSONL_DIR}/${filename}" ]]; then
        bail "expected file ${TEST_OUTPUT_JSONL_DIR}/${filename} is missing in the actual output."
    fi
done

# Make sure all files in the actual output directory are same as expected.
for f in "${TEST_OUTPUT_JSONL_DIR}"/*; do
    expected="${EXPECTED_DIR}/$(basename "${f}")"
    if [[ ! -f ${expected} ]]; then
        bail "${f} in the actual output is not expected."
    fi

    jq --sort-keys -c <"${f}" >"${f}.normalized" || bail "Failed to normalize ${f}."

    diff --suppress-common-lines --side-by-side "${f}.normalized" "${expected}" \
    || bail "Test Failed"
done


echo "Tests Passed!"