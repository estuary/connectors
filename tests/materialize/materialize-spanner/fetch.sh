#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

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

function exportToJsonl() {
  local table_name="$1"
  local display_name=$(echo "$table_name" | tr -d '"' | tr -d '`')

  # Query Spanner using gcloud and convert to JSON
  local query="SELECT * FROM $table_name ORDER BY id, flow_published_at"

  gcloud spanner databases execute-sql "$SPANNER_DATABASE" \
    --instance="$SPANNER_INSTANCE_ID" \
    --project="$SPANNER_PROJECT_ID" \
    --sql="$query" \
    --format=json | jq "{ \"_table\": \"$display_name\", rows: [\
      .metadata.rowType.fields as \$fields |\
      .rows[] |\
      [., \$fields] |\
      .[1] as \$f |\
      .[0] |\
      to_entries |\
      map({key: \$f[.key].name, value: .value}) |\
      from_entries]\
    }"
}

exportToJsonl "simple"
exportToJsonl "duplicate_keys_standard"
exportToJsonl "multiple_types"
exportToJsonl "formatted_strings"
exportToJsonl "symbols"
exportToJsonl "unsigned_bigint"
exportToJsonl "deletions"
exportToJsonl "string_escaped_key"
exportToJsonl "all_key_types_part_one"
exportToJsonl "all_key_types_part_two"
exportToJsonl "all_key_types_part_three"
exportToJsonl "fields_with_projections"
exportToJsonl "many_columns"
exportToJsonl "timezone_datetimes_standard"
