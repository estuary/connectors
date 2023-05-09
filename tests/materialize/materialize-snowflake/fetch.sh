#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

function exportToJsonl() {
  go run ${TEST_DIR}/materialize-snowflake/fetch-data.go "$1"
}

exportToJsonl "Simple"
exportToJsonl "duplicate_keys_standard"
exportToJsonl "duplicate_keys_delta"
exportToJsonl "duplicate_keys_delta_exclude_flow_doc"
exportToJsonl "\"Multiple Types\""
exportToJsonl "\"Formatted Strings\""
