#!/bin/bash

set -e

function dropTable() {
    go run ${TEST_DIR}/materialize-snowflake/fetch-data.go --delete "$1"
}

# Remove materialized tables.
dropTable "Simple"
dropTable "duplicate_keys_standard"
dropTable "duplicate_keys_delta"
dropTable "duplicate_keys_delta_exclude_flow_doc"
dropTable "\"Multiple Types\""
dropTable "\"Formatted Strings\""

# Remove the persisted materialization spec & checkpoint for this test materialization so subsequent
# runs start from scratch.
go run ${TEST_DIR}/materialize-snowflake/fetch-data.go --delete-specs notable
