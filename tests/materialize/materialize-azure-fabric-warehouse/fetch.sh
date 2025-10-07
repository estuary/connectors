#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

function exportToJsonl() {
  go run "${TEST_DIR}"/materialize-azure-fabric-warehouse/fetch-data.go "$1" | jq "{ "_table": \"$1\", rows: . }"
}

exportToJsonl "simple"
exportToJsonl "duplicate_keys_standard"
exportToJsonl "duplicate_keys_delta"
exportToJsonl "multiple_types"
exportToJsonl "formatted_strings"
exportToJsonl "deletions"
exportToJsonl "binary_key"
exportToJsonl "string_escaped_key"
exportToJsonl "all_key_types_part_one"
exportToJsonl "all_key_types_part_two"
exportToJsonl "all_key_types_part_three"
exportToJsonl "fields_with_projections"
exportToJsonl "many_columns"
