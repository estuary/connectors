#!/bin/bash

set -e

function dropTable() {
    local table_name="$1"
    gcloud spanner databases ddl update "$SPANNER_DATABASE" \
        --instance="$SPANNER_INSTANCE_ID" \
        --project="$SPANNER_PROJECT_ID" \
        --ddl="DROP TABLE $table_name" 2>/dev/null || echo "Table $table_name not found or already dropped"
}

# Remove materialized tables.
dropTable "simple"
dropTable "duplicate_keys_standard"
dropTable "duplicate_keys_delta"
dropTable "duplicate_keys_delta_exclude_flow_doc"
dropTable '`duplicate keys @ with spaces`'
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

# Cleanup temporary key file if it was created
if [ -n "$SPANNER_KEY_FILE" ] && [ -f "$SPANNER_KEY_FILE" ]; then
    rm -f "$SPANNER_KEY_FILE"
fi
