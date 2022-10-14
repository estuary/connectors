#!/bin/bash
set -e

if [ $# -ne 1 ]
then
    echo "execution using: $0 <test-output-jsonl-dir>"
    exit 1
fi

# The relative path to ${TEST_DIR} to store final results.
result_dir="$1"

echo "Waiting 20 seconds for bigquery data to be available for fetching"
sleep 20

function exportToJsonl() {
    go run ${TEST_BASE_DIR}/materialize-bigquery/fetch-data.go "$1"
}

exportToJsonl "simple" > "${TEST_DIR}/${result_dir}/simple.jsonl"
exportToJsonl "duplicate_keys" > "${TEST_DIR}/${result_dir}/duplicated-keys.jsonl"
exportToJsonl "multiple_types" > "${TEST_DIR}/${result_dir}/multiple-data-types.jsonl"
