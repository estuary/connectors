#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

function exportToJsonl() {
  query "select * from \"$1\";" | jq -c ".[] | del(.flow_document) | { table: \"$1\", row: . }"
}

exportToJsonl "Simple"
exportToJsonl "duplicate_keys_standard"
exportToJsonl "duplicate_keys_delta"
exportToJsonl "duplicate_keys_delta_exclude_flow_doc"
exportToJsonl "Multiple Types"
exportToJsonl "Formatted Strings"
