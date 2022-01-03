#!/bin/bash

set -em

ROOT_DIR="$(git rev-parse --show-toplevel)"
cd "$ROOT_DIR"


function bail() {
   echo "$@" 1>&2
   echo "Test failed."
   exit 1
}

test -n "$CONNECTOR" || bail "must specify CONNECTOR env variable"
test -n "$VERSION" || bail "must specify VERSION env variable"

# The dir of materialize tests.
TEST_SCRIPTS_DIR="${ROOT_DIR}/tests/materialize"

# Post-templating catalog source processed by the test.
CATALOG="${TEST_SCRIPTS_DIR}/materialize-test.flow.yaml"

# The dir containing test scripts of the connector.
CONNECTOR_TEST_SCRIPTS_DIR="${TEST_SCRIPTS_DIR}/${CONNECTOR}"

# Connector image to use. Export to make it available to `envsubst`
export CONNECTOR_IMAGE="ghcr.io/estuary/${CONNECTOR}:${VERSION}"
echo "testing connector: '$CONNECTOR'"

function bail() {
    echo "$@" 1>&2
    exit 1
}
export -f bail

test -n "${CONNECTOR}" || bail "must specify CONNECTOR env variable"
test -n "${VERSION}" || bail "must specify VERSION env variable"

# Directory under which the test runs.
TESTDIR=".build/tests/${CONNECTOR}"

# Ensure we start with an empty dir, since temporary data plane files will go here.
# Remove it, if it exists already.
if [[ -d "${TESTDIR}" ]]; then
    rm -r ${TESTDIR}
fi
mkdir -p "${TESTDIR}"

# Map to an absolute directory.
export TESTDIR=$(realpath ${TESTDIR})
# `flowctl` commands look for a BUILDS_ROOT environment variable which roots
# build databases known to the data plane.
export BUILDS_ROOT="file://${TESTDIR}/"
# `flowctl` commands which interact with the data plane look for *_ADDRESS
# variables, which are created by the temp-data-plane we're about to start.
export BROKER_ADDRESS=unix://localhost${TESTDIR}/gazette.sock
export CONSUMER_ADDRESS=unix://localhost${TESTDIR}/consumer.sock

# The path to the data ingestion go script.
export DATA_INGEST_SCRIPT_PATH="${TEST_SCRIPTS_DIR}/datasets/ingest.go"
export DATA_INGEST_ENDPOINT=unix://${TESTDIR}/consumer.sock

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

# Util function for running the `flowctl combine` to combine the results.
function combineResults() {
    # The collection name specified in the catalog.
    local collection="$1"
    # The input jsonl file contains json docs of the specified collection.
    local input_file_name="$2"
    # The output jsonl file of the combined results.
    local output_file_name="$3"
    ${TESTDIR}/flowctl combine \
      --collection "${collection}" \
      --source "file://${CATALOG}" \
      < "${TESTDIR}/${input_file_name}" \
      > "${TESTDIR}/${output_file_name}" \
      || bail "combine results failed."
}
# Export the function to be avialble to connector-specific testing scripts.
export -f combineResults

FLOW_IMAGE="ghcr.io/estuary/flow:dev"
docker pull ${FLOW_IMAGE}
FLOW_CONTAINER=$(docker create $FLOW_IMAGE)
docker cp ${FLOW_CONTAINER}:/usr/local/bin/flowctl ${TESTDIR}/flowctl
docker cp ${FLOW_CONTAINER}:/usr/local/bin/etcd ${TESTDIR}/etcd
docker cp ${FLOW_CONTAINER}:/usr/local/bin/gazette ${TESTDIR}/gazette
docker rm ${FLOW_CONTAINER}

${TESTDIR}/flowctl temp-data-plane \
    --log.level info \
    --tempdir ${TESTDIR} \
    --unix-sockets \
    &
DATA_PLANE_PID=$!

# Arrange to stop the data plane and cleanup on exit.
trap "kill -s SIGTERM ${DATA_PLANE_PID} && wait ${DATA_PLANE_PID} && ${CONNECTOR_TEST_SCRIPTS_DIR}/cleanup.sh" EXIT

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
> "${CATALOG}" || bail "generating ${CATALOG} failed."

echo -e "\nbuilding test catalog"
${TESTDIR}/flowctl api build --directory ${TESTDIR}/builds --build-id test-build-id --source ${CATALOG} --ts-package || bail "Build failed."
echo -e "\nactivating test catalog"
${TESTDIR}/flowctl api activate --build-id test-build-id --all --log.level info || bail "Activate failed."

echo -e "\nexecuting ingest-data"
source "${CONNECTOR_TEST_SCRIPTS_DIR}/ingest-data.sh" || bail "ingest data failed."

echo -e "\nwait catalog to finish."
${TESTDIR}/flowctl api await --build-id test-build-id --log.level info || bail "Await failed."

# The relative path to ${TESTDIR} for storing result files.
result_dir=result_jsonl
mkdir -p "$(realpath "${TESTDIR}"/"${result_dir}")"

echo -e "\nexecuting fetch-materialize-results"
source "${CONNECTOR_TEST_SCRIPTS_DIR}/fetch-materialize-results.sh" ${result_dir}\
    || bail "fetching test results failed."

echo -e "\ncomparing actual v.s. expected"
# The dir that contains all the files expected from the output.
EXPECTED_DIR="${CONNECTOR_TEST_SCRIPTS_DIR}/expected/"

# Make sure all expected files has their corresponding files in the actual output directory.
for f in "${EXPECTED_DIR}"/*; do
    filename="$(basename "${f}")"
    if [[ ! -f "${TESTDIR}/${result_dir}/${filename}" ]]; then
        bail "expected file ${result_dir}/${filename} is missing in the actual output."
    fi
done

# Make sure all files in the actual output directory are same as expected.
for f in "${TESTDIR}/${result_dir}"/*; do
    expected="${EXPECTED_DIR}/$(basename "${f}")"
    if [[ ! -f ${expected} ]]; then
        bail "${f} in the actual output is not expected."
    fi

    jq --sort-keys -c <"${f}" >"${f}.normalized" || bail "Failed to normalize ${f}."

    diff --suppress-common-lines --side-by-side "${f}.normalized" "${expected}" \
    || bail "Test Failed"
done


echo "Tests Passed!"
