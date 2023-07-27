#!/bin/bash

set -e

function dropTable() {
    go run ${TEST_DIR}/materialize-bigquery/fetch-data.go --delete "$1"
}

# Remove materialized tables.
dropTable "simple"
dropTable "duplicate_keys"
dropTable "multiple_types"
dropTable "formatted_strings"

# Remove the persisted materialization spec & checkpoint for this test materialization so subsequent
# runs start from scratch.
go run ${TEST_DIR}/materialize-bigquery/fetch-data.go --delete-specs notable
