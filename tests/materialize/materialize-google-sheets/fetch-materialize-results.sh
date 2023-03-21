#!/bin/bash
set -e

if [ $# -ne 1 ]; then
    echo "execution using: $0 <test-output-jsonl-dir>"
    exit 1
fi

# The relative path to ${TEST_DIR} to store final results.
result_dir="$1"

# Wait for enough long to have data available for fetching.
# TODO(johnny): gross.
sleep 2

function exportToJsonl() {
    export SHEET_NAME="$1"

    go run ${TEST_BASE_DIR}/materialize-google-sheets/fetch-sheets.go
}

exportToJsonl "Simple" >"${TEST_DIR}/${result_dir}/simple.jsonl"
exportToJsonl "duplicate_keys" >"${TEST_DIR}/${result_dir}/duplicated-keys-non-delta.jsonl"
exportToJsonl "Multiple Types" >"${TEST_DIR}/${result_dir}/multiple-data-types.jsonl"
