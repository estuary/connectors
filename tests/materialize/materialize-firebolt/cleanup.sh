#!/bin/bash

set -e

function dropTable() {
    go run ${TEST_DIR}/materialize-firebolt/fetch-data.go --delete "$1"
}

# Remove materialized tables.
dropTable "simple"
dropTable "duplicate_keys_standard"
dropTable "formatted_strings"
dropTable "long_string"
dropTable "deletions"

# Remove external tables
dropTable "simple_external"
dropTable "duplicate_keys_standard_external"
dropTable "formatted_strings_external"
dropTable "long_string_external"
dropTable "deletions_external"
