#!/bin/bash

set -e

# Flow image / container name used in testing.
FLOW_IMAGE="ghcr.io/estuary/flow:dev"
# Prefix of the containers running flowctl via Flow image.
FLOW_CONTAINER_NAME_PREFIX=flowctl-${CONNECTOR}

# The docker image of the connector to be tested.
# The connector image needs to be available to envsubst.
export CONNECTOR_IMAGE="ghcr.io/estuary/${CONNECTOR}:${VERSION}"

function bail() {
    echo "$@" 1>&2
    # Output logs for debugging if the execution fails for any reason.
    for name in $(docker ps -a -f name=${FLOW_CONTAINER_NAME_PREFIX} -q); do
        echo -e "\nlogs from container ${name}"
        docker logs "${name}"
    done

    echo "test failed..."
    exit 1
}
export -f bail

test -n "${CONNECTOR}" || bail "must specify CONNECTOR env variable"
test -n "${VERSION}" || bail "must specify VERSION env variable"

# The directory of the connectors github repo.
ROOT_DIR="$(git rev-parse --show-toplevel)"

# The dir of materialize tests.
TEST_SCRIPTS_DIR="${ROOT_DIR}/tests/materialize"

# The dir containing test scripts of the connector.
CONNECTOR_TEST_SCRIPTS_DIR="${TEST_SCRIPTS_DIR}/${CONNECTOR}"

# Export envvars to be shared by all scripts.

# A directory for hosting temp files generated during the test executions.
export TEST_DIR="$(mktemp -d -t "${CONNECTOR}"-XXXXXX)"

# Broker and onsumer addresses of Flow services.
export BROKER_ADDRESS=localhost:8080
export CONSUMER_ADDRESS=localhost:9000

# The path to the data ingestion go script.
export DATA_INGEST_SCRIPT_PATH="${TEST_SCRIPTS_DIR}/datasets/ingest.go"

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

# Util function to run flowctl command via docker.
function runFlowctl() {
    local script="$1"
    local args=$2
    local detached
    if [[ "$3" == true ]]; then
      detached="-d"
    fi

    local test_scripts_dir_target=/home/flow/scripts
    # Run as root, not the flow user, since the user within the container needs to access the
    # docker socket.

    docker run ${detached:+"$detached"} \
        --name "${FLOW_CONTAINER_NAME_PREFIX}-${script}-${EPOCHREALTIME}" \
        --user 0  \
        --mount type=bind,source=/var/run/docker.sock,target=/var/run/docker.sock \
        --mount type=bind,source="${TEST_SCRIPTS_DIR}",target=${test_scripts_dir_target} \
        --mount type=bind,source="${TEST_DIR}",target=${TEST_DIR} \
        --mount type=bind,source=/tmp,target=/tmp \
        --env BROKER_ADDRESS="http://${BROKER_ADDRESS}" \
        --env CONSUMER_ADDRESS="http://${CONSUMER_ADDRESS}" \
        --env TEST_DIR \
        --env BUILDS_ROOT="file://${TEST_DIR}/build/" \
        --env BUILD_ID=run-test-"${CONNECTOR}" \
        --env CATALOG \
        --network=host \
        "${FLOW_IMAGE}" \
        "${test_scripts_dir_target}/flowctl/${script}" \
        "${args}" \
        >/dev/null 2>&1
}

# Util function for running the `flowctl combine` to combine the results.
function combineResults() {
    # The collection name specified in the catalog.
    local collection="$1"
    # The input jsonl file contains json docs of the specified collection.
    local input_file_name="$2"
    # The output jsonl file of the combined results.
    local output_file_name="$3"
    runFlowctl "combine.sh" "${collection} ${input_file_name} ${output_file_name}" false || bail "combine results failed."
}
# Export the function to be avialble to connector-specific testing scripts.
export -f combineResults

function cleanup() {
    echo -e "\nexecuting cleanup"

    runFlowctl delete.sh "" false || true

    source "${CONNECTOR_TEST_SCRIPTS_DIR}/cleanup.sh" || true
    for name in $(docker ps -a -f name="${FLOW_CONTAINER_NAME_PREFIX}" -q); do
        docker rm -f "${name}"
    done
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
escaped_connector_config=$(echo "${CONNECTOR_CONFIG}" | sed 's/\//\\\//g')
envsubst < ${ROOT_DIR}/tests/materialize/flow.json.template | yq eval -P | \
sed "s/CONNECTOR_CONFIG_PLACEHOLDER/${escaped_connector_config}/g" \
> "${TEST_DIR}/${CATALOG}" || bail "generating ${CATALOG} failed."

echo -e "\nstarting temp data plane"
runFlowctl temp-data-plane.sh "" true || bail "starting temp data plane."

echo -e "\nbuilding and activating test catalog"
runFlowctl build-and-activate.sh "" false || bail "building and activating test catalog failed."

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
    || bail "Test Failed"
done


echo "Tests Passed!"
