#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

function exportToJsonl() {
  go run ${TEST_DIR}/materialize-bigquery/fetch-data.go "$1" | jq -sc "{ \"table\": \"$1\", rows: . }"
}

exportToJsonl "simple"
exportToJsonl "duplicate_keys"
exportToJsonl "multiple_types"
exportToJsonl "formatted_strings"
exportToJsonl "unsigned_bigint"
exportToJsonl "deletions"
exportToJsonl "string_escaped_key"
exportToJsonl "all_key_types_part_one"
exportToJsonl "all_key_types_part_two"
exportToJsonl "all_key_types_part_three"
