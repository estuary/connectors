#!/bin/bash

set -e

# Decrypt config and set credentials from config.yaml
CONFIG_JSON="$(decrypt_config $CONNECTOR_TEST_DIR/config.yaml)"
SPANNER_PROJECT_ID="$(echo "$CONFIG_JSON" | jq -r .project_id)"
SPANNER_INSTANCE_ID="$(echo "$CONFIG_JSON" | jq -r .instance_id)"
SPANNER_DATABASE="$(echo "$CONFIG_JSON" | jq -r .database)"
SPANNER_SERVICE_ACCOUNT_JSON="$(echo "$CONFIG_JSON" | jq -r '.credentials.service_account_json')"

# Set up gcloud authentication using service account credentials
SPANNER_KEY_FILE=$(mktemp)
echo "$SPANNER_SERVICE_ACCOUNT_JSON" > "$SPANNER_KEY_FILE"
gcloud auth activate-service-account --key-file="$SPANNER_KEY_FILE" --quiet

function cleanup_keyfile() {
  rm -f "$SPANNER_KEY_FILE"
}
trap cleanup_keyfile EXIT

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
