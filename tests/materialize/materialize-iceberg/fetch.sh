#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

function exportToJsonl() {
  ${ICEBERG_HELPER_CMD} read ${NAMESPACE}."$1"_${TABLE_SUFFIX} | jq -s "sort_by([.id, .flow_published_at]) | { _table: \"$1\", rows: . }"
}

exportToJsonl "simple"
exportToJsonl "duplicate_keys_standard"
exportToJsonl "multiple_types"
exportToJsonl "formatted_strings"
exportToJsonl "deletions"
exportToJsonl "binary_key"
exportToJsonl "string_escaped_key"
