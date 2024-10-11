#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

function exportToJsonl() {
  go run "${TEST_DIR}"/materialize-firebolt/fetch-data.go "$1" | jq "{ "_table": \"$1\", rows: map(del(.flow_document)) }"
}

exportToJsonl "simple"
exportToJsonl "duplicate_keys_standard"
exportToJsonl "formatted_strings"
exportToJsonl "long_string"
exportToJsonl "deletions"
