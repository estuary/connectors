#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

function exportToJsonl() {
  name=$(echo "$1" | tr -d '"')
  go run ${TEST_DIR}/materialize-snowflake/fetch-data.go "$1" | jq "{ "_table": \"$name\", rows: . }"
}

exportToJsonl "simple"
exportToJsonl "duplicate_keys_standard"
exportToJsonl "duplicate_keys_delta"
exportToJsonl "duplicate_keys_delta_exclude_flow_doc"
exportToJsonl '"duplicate keys @ with spaces"'
exportToJsonl "multiple_types"
exportToJsonl "formatted_strings"
exportToJsonl "symbols"
exportToJsonl "unsigned_bigint"
exportToJsonl "deletions"
exportToJsonl "string_escaped_key"
exportToJsonl "all_key_types_part_one"
exportToJsonl "all_key_types_part_two"
exportToJsonl "all_key_types_part_three"
