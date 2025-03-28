#!/bin/bash
set -e

# Read all docs from elastic search indices to jsonl output.
function exportIndexToJsonl() {
    local index="$1"

    curl -u "${TEST_ES_USERNAME}":"${TEST_ES_PASSWORD}" -o /dev/null -s "${TEST_ES_LOCAL_ENDPOINT}/${index}/_refresh"

    curl -u "${TEST_ES_USERNAME}":"${TEST_ES_PASSWORD}" -s "${TEST_ES_LOCAL_ENDPOINT}/${index}/_search?size=100" |
        jq "[.hits | .hits[] | ._source ] | sort_by(.id, .flow_published_at) | { index: \"$index\", rows: . }"
}

exportIndexToJsonl index-simple
exportIndexToJsonl index-duplicated-keys-standard
exportIndexToJsonl index-duplicated-keys-delta
exportIndexToJsonl index-duplicated-keys-delta-exclude-flow-doc
exportIndexToJsonl index-multiple-data-types
exportIndexToJsonl index-formatted-strings
exportIndexToJsonl index-deletions
