#!/bin/bash

set -e

if [[ $# < 1 ]]; then
    echo "This script generates a built protobuf spec from a Flow materialization specification."
    echo "Usage: generate-spec-proto.sh <path/to/spec.yaml>"
    exit 1
fi

command -v flowctl >/dev/null 2>&1 || {
    echo >&2 "flowctl must be available via PATH, aborting."
    exit 1
}

command -v sqlite3 >/dev/null 2>&1 || {
    echo >&2 "sqlite3 must be available via PATH, aborting."
    exit 1
}

SOURCE=$1
BASE=$(basename -- "$SOURCE")
OUTPUT=$(dirname $1)"/generated_specs/${BASE%.*}".proto

TEMP_DIR=$(mktemp -d /tmp/build-XXXXXX)
function cleanup {
    rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

flowctl raw build \
    --build-id 12345 \
    --db-path ${TEMP_DIR}/build.db \
    --source $1

errors=$(sqlite3 ${TEMP_DIR}/build.db "select * from errors")
if [[ -n "$errors" ]]; then
    echo "$errors" >&2
    exit 1
fi

sqlite3 ${TEMP_DIR}/build.db "select writefile('${OUTPUT}', spec) from built_materializations;"
