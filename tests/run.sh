#!/bin/bash

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

function pollDevelop() {
    local catalog="$1"
    local directory="$2"
    docker run --user 0 -it --rm --mount type=bind,source=/tmp,target=/tmp --mount type=bind,source=/var/run/docker.sock,target=/var/run/docker.sock --mount type=bind,source=`pwd`,target=/home/flow/project --network=host "${FLOW_IMAGE}" flowctl develop --poll --source "$catalog" --directory "$directory"
}

export -f pollDevelop

echo "testing connector: '$CONNECTOR'"
source "tests/${CONNECTOR}/setup.sh" || bail "${CONNECTOR}/setup.sh not found"
trap ./tests/${CONNECTOR}/cleanup.sh EXIT

test_dir=".build/tests/${CONNECTOR}"
catalog="${test_dir}/test.flow.yaml"
sqlite_path="${test_dir}/materialization.db"
actual="${test_dir}/actual_test_results.txt"

mkdir -p "$test_dir"
# ensure we start with an empty dir, since the flowctl_develop directory will go here
rm -rf "${test_dir}/*"
cat tests/template.flow.yaml | CONNECTOR_IMAGE="ghcr.io/estuary/${CONNECTOR}:${VERSION}" envsubst > "$catalog"
pollDevelop "$catalog" "$test_dir"

sqlite3 -header "$sqlite_path" "select id, canary from test_results;" > "$actual"
# diff will exit 1 if files are different
diff --brief --side-by-side "$actual" "tests/${CONNECTOR}/expected.txt" || bail "Test Failed"

# delete all the data in the test_results table so that we can assert that the connector doesn't add
# any more on a subsequent invocation.
sqlite3 "$sqlite_path" "delete from test_results;"

pollDevelop "$catalog" "$test_dir"

num_rows="$(sqlite3 "$sqlite_path" 'select count(*) from test_results;')"
if [[ "$num_rows" != "0" ]]; then
    bail expected test_results to be empty on subsequent run, but got $num_rows rows
fi

echo "Test Passed"
