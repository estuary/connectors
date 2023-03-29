#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

function exportToJsonl() {
    export SHEET_NAME="$1"
    export SHEET_COMMAND=fetch

    go run ${TEST_DIR}/materialize-google-sheets/sheet-helper.go
}

exportToJsonl "Simple"
exportToJsonl "duplicate_keys"
exportToJsonl "Multiple Types"
