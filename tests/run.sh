#!/bin/bash
#
# This script executes an end-to-end integration test of a flow catalog with a given connector.
# The connector name is read from the CONNECTOR env variable, and the image tag is read from
# VERSION. Both of those are required.
# The tests will execute a connector-specific setup script, then run `flowctl develop --poll` with a
# generated catalog that uses the connector to capture some data.

set -e

ROOT_DIR="$(git rev-parse --show-toplevel)"
cd "$ROOT_DIR"

function bail() {
    echo "$@" 1>&2
    exit 1
}

test -n "$CONNECTOR" || bail "must specify CONNECTOR env variable"
test -n "$VERSION" || bail "must specify VERSION env variable"

FLOW_IMAGE="quay.io/estuary/flow:v0.1.0-376-g273cd3d"
# the connector image needs to be available to envsubst
export CONNECTOR_IMAGE="ghcr.io/estuary/${CONNECTOR}:${VERSION}" 

function pollDevelop() {
    local catalog="$1"
    local directory="$2"
    # run as root, not the flow user, since the user within the container needs to access the
    # docker socket.
    docker run --user 0 --rm \
        --mount type=bind,source=/tmp,target=/tmp \
        --mount type=bind,source=/var/run/docker.sock,target=/var/run/docker.sock \
        --mount type=bind,source=`pwd`,target=/home/flow/project \
        --network=host \
        "${FLOW_IMAGE}" \
        flowctl develop --poll --source "$catalog" --directory "$directory" \
        || bail "flowctl develop failed"
    echo -e "\nfinished polling"
}

export -f pollDevelop

test_dir=".build/tests/${CONNECTOR}"
catalog="${test_dir}/test.flow.yaml"
sqlite_path="${test_dir}/materialization.db"
actual="${test_dir}/actual_test_results.txt"

# Ensure we start with an empty dir, since the flowctl_develop directory will go here.
# Unfortunately, we can't just delete it, since the flowctl container is running as root, and so
# all the files it creates will be owned by root. This isn't an issue in CI, since this directory
# won't exist as long as we do a clean checkout, but for local runs, I'd rather error out than to
# run 'sudo rm'.
if [[ -d "${test_dir}" ]]; then
    bail "The test directory must not already exist prior to a test run"
fi
mkdir -p "$test_dir"

echo "testing connector: '$CONNECTOR'"
# First step is to get the spec from the connector and ensure it's valid json
docker run --rm "$CONNECTOR_IMAGE" spec | jq -cM || bail "failed to validate spec"

echo -e "\nexecuting setup"
source "tests/${CONNECTOR}/setup.sh" || bail "${CONNECTOR}/setup.sh failed"
if [[ -z "$STREAM" ]]; then
    bail "setup did not set STREAM"
fi
if [[ -z "$CONNECTOR_CONFIG" ]]; then
    bail "setup did not set CONNECTOR_CONFIG"
fi
trap ./tests/${CONNECTOR}/cleanup.sh EXIT

cat tests/template.flow.yaml | envsubst > "$catalog"
pollDevelop "$catalog" "$test_dir"

sqlite3 -header "$sqlite_path" "select id, canary from test_results;" > "$actual"
# diff will exit 1 if files are different
diff --suppress-common-lines --side-by-side "$actual" "tests/${CONNECTOR}/expected.txt" || bail "Test Failed"

# This second run is commented out because there's a flowctl bug that causes fragments to not be
# persisted on shutdown. This should be re-enabled once that's fixed.
# delete all the data in the test_results table so that we can assert that the connector doesn't add
# any more on a subsequent invocation.
#sqlite3 "$sqlite_path" "delete from test_results;"

#pollDevelop "$catalog" "$test_dir"

#num_rows="$(sqlite3 "$sqlite_path" 'select count(*) from test_results;')"
#if [[ "$num_rows" != "0" ]]; then
#    bail expected test_results to be empty on subsequent run, but got $num_rows rows
#fi

echo "Test Passed"
