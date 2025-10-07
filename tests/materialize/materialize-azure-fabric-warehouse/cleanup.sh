#!/bin/bash

set -e

function dropTable() {
    go run ${TEST_DIR}/materialize-azure-fabric-warehouse/fetch-data.go --delete "$1"
}

dropTable "simple"
dropTable "duplicate_keys_standard"
dropTable "duplicate_keys_delta"
dropTable "multiple_types"
dropTable "formatted_strings"
dropTable "deletions"
dropTable "binary_key"
dropTable "string_escaped_key"
dropTable "all_key_types_part_one"
dropTable "all_key_types_part_two"
dropTable "all_key_types_part_three"
dropTable "fields_with_projections"
dropTable "many_columns"

go run ${TEST_DIR}/materialize-azure-fabric-warehouse/fetch-data.go --delete-checkpoint notable
