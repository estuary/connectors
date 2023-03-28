#!/bin/bash
set -e

# Read all docs from elastic search indices to jsonl output.
function exportIndexToJsonl() {
    local index="$1"
    local sort_by="$2"

    curl -s "${TEST_ES_LOCAL_ENDPOINT}/${index}/_search" \
    | jq "[.hits | .hits[] | ._source | del(._meta)] | sort_by(.${sort_by}) | { index: \"$index\", rows: . }"
}

# Sleep because elastic is really bad about read-committed semantics.
sleep 1

exportIndexToJsonl "${TEST_ES_INDEX_DUPLICATED_KEYS_DELTA}" id
exportIndexToJsonl "${TEST_ES_INDEX_DUPLICATED_KEYS_NON_DELTA}" id
exportIndexToJsonl "${TEST_ES_INDEX_MULTIPLE_DATA_TYPES}" id
