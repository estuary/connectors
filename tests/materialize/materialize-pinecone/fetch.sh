#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

function exportToJsonl() {
  go run ${TEST_DIR}/materialize-pinecone/fetch-data.go "$1"
}

exportToJsonl "simple"
exportToJsonl "duplicated-keys"
exportToJsonl "multiple-types"
