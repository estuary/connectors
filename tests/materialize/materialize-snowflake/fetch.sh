#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

function exportToJsonl() {
  go run ${TEST_DIR}/materialize-snowflake/fetch-data.go "$1" | jq "{ "_table": \"$1\", rows: . }"
}

exportToJsonl "simple"
exportToJsonl "duplicate_keys_standard"
exportToJsonl "duplicate_keys_delta"
exportToJsonl "duplicate_keys_delta_exclude_flow_doc"
exportToJsonl "multiple_types"
exportToJsonl "formatted_strings"
exportToJsonl "symbols"
exportToJsonl "unsigned_bigint"
