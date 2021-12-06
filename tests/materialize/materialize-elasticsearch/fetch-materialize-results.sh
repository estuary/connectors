#!/bin/bash
set -e

if [ $# -ne 1 ]
then
    echo "execution using: $0 <test-output-jsonl-dir>"
    exit 1
fi

# The relative path to ${TEST_DIR} to store final results.
result_dir="$1"

sleep 2

# Read all docs from elastic search indices to jsonl output.
function exportIndexToJsonl() {
    local index="$1"
    local sort_by="$2"
    local jsonl_path="$3"

    curl -s "${TEST_ES_ENDPOINT}/${index}/_search" \
    | jq -c "[.hits | .hits[] | ._source | del(._meta)] | sort_by(.${sort_by}) | .[]" \
    > "${jsonl_path}"
}

exportIndexToJsonl "${TEST_ES_INDEX_DUPLICATED_KEYS_DELTA}" id "${TEST_DIR}/${result_dir}/duplicated-keys-delta.jsonl"
exportIndexToJsonl "${TEST_ES_INDEX_DUPLICATED_KEYS_NON_DELTA}" id "${TEST_DIR}/${result_dir}/duplicated-keys-non-delta.jsonl"
exportIndexToJsonl "${TEST_ES_INDEX_MULTIPLE_DATA_TYPES}" id "${TEST_DIR}/${result_dir}/multiple-data-types.jsonl"