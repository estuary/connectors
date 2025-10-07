#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

function exportToJsonl() {
  query "select * from \`$1\`;" | jq -c ".rows | { table: \"$1\", rows: map(del(.flow_document)) }"
}

exportToJsonl "Simple"
exportToJsonl "duplicate_keys_standard"
exportToJsonl "duplicate_keys_delta"
exportToJsonl "duplicate_keys_delta_exclude_flow_doc"
exportToJsonl "Multiple Types"
exportToJsonl "Formatted Strings"
exportToJsonl "long-string"
exportToJsonl "unsigned_bigint"
exportToJsonl "deletions"
exportToJsonl "all_key_types_part_one"
exportToJsonl "all_key_types_part_two"
exportToJsonl "all_key_types_part_three"
exportToJsonl "fields_with_projections"
exportToJsonl "many_columns"
