#!/bin/bash
set -e

if [ $# -ne 1 ]
then
    echo "execution using: $0 <test-output-jsonl-dir>"
    exit 1
fi

# The relative path to ${TEST_DIR} to store final results.
result_dir="$1"

# Wait for enough long to have data available for fetching.
# TODO(johnny): gross.
sleep 2

function exportToJsonl() {
  docker exec \
    -e PGUSER=$PGUSER \
    -e PGPASSWORD=$PGPASSWORD \
    -e PGDATABASE=$PGDATABASE \
    materialize-postgres-postgres-1 psql \
    --tuples-only \
    --command "select row_to_json(t) from \"$1\" as t;" |
        jq -c 'del(.flow_document)'
}

exportToJsonl "Simple" > "${TEST_DIR}/${result_dir}/simple.jsonl"
exportToJsonl "duplicate_keys_standard" > "${TEST_DIR}/${result_dir}/duplicated-keys-standard.jsonl"
exportToJsonl "duplicate_keys_delta" > "${TEST_DIR}/${result_dir}/duplicated-keys-delta.jsonl"
exportToJsonl "Multiple Types" > "${TEST_DIR}/${result_dir}/multiple-data-types.jsonl"
