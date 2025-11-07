#!/bin/bash

set -e

touch ${TEMP_DIR}/drop.sql
echo '' > ${TEMP_DIR}/drop.sql

function dropTable() {
    echo "DROP TABLE IF EXISTS $1;" >> ${TEMP_DIR}/drop.sql
}

# Remove materialized tables.
dropTable "simple"
dropTable "duplicate_keys_standard"
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
dropTable "perf_simple"
dropTable "perf_uuid_key"

echo "dropping all tables in Spanner database '$SPANNER_DATABASE'..."
gcloud spanner databases ddl update "$SPANNER_DATABASE" \
    --instance="$SPANNER_INSTANCE_ID" \
    --project="$SPANNER_PROJECT_ID" \
    --ddl-file="${TEMP_DIR}/drop.sql"

rm ${TEMP_DIR}/drop.sql

# Cleanup temporary key file if it was created
if [ -n "$SPANNER_KEY_FILE" ] && [ -f "$SPANNER_KEY_FILE" ]; then
    rm -f "$SPANNER_KEY_FILE"
fi
