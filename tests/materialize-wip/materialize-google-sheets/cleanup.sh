#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

export SHEET_NAME=any
export SHEET_COMMAND=cleanup

go run ${TEST_DIR}/materialize-google-sheets/sheet-helper.go
