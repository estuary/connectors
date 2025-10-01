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
dropTable "unsigned_bigint"
dropTable "deletions"
dropTable "string_escaped_key"
dropTable "all_key_types_part_one"
dropTable "all_key_types_part_two"
dropTable "all_key_types_part_three"
dropTable "fields_with_projections"

# Remove the persisted materialization spec & checkpoint for this test materialization so subsequent
# runs start from scratch.
go run ${TEST_DIR}/materialize-bigquery/fetch-data.go --delete-specs notable
