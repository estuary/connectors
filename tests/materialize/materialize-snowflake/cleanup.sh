#!/bin/bash

set -e

function dropTable() {
    go run ${TEST_DIR}/materialize-snowflake/fetch-data.go --delete "$1"
}

# Remove materialized tables.
dropTable "simple"
dropTable "duplicate_keys_standard"
dropTable "duplicate_keys_delta"
dropTable "duplicate_keys_delta_exclude_flow_doc"
dropTable '"duplicate keys @ with spaces"'
dropTable "multiple_types"
dropTable "formatted_strings"
dropTable "symbols"
dropTable "unsigned_bigint"
dropTable "deletions"
dropTable "string_escaped_key"
dropTable "all_key_types_part_one"
dropTable "all_key_types_part_two"
dropTable "all_key_types_part_three"
dropTable "fields_with_projections"
dropTable "many_columns"
dropTable "timezone_datetimes_standard"
dropTable "timezone_datetimes_delta"
dropTable "perf_simple"
dropTable "perf_uuid_key"
