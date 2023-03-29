#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

function exportToJsonl() {
    go run ${TEST_DIR}/materialize-bigquery/fetch-data.go "$1"
}

exportToJsonl "simple"
exportToJsonl "duplicate_keys"
exportToJsonl "multiple_types"
exportToJsonl "formatted_strings"
