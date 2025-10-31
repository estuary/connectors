#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

function exportToJsonl() {
  local table_name="$1"
  local display_name=$(echo "$table_name" | tr -d '"' | tr -d '`')

  # Query Spanner using gcloud and convert to JSON
  local query="SELECT * FROM $table_name ORDER BY id, flow_published_at"

  gcloud spanner databases execute-sql "$SPANNER_DATABASE" \
    --instance="$SPANNER_INSTANCE_ID" \
    --project="$SPANNER_PROJECT_ID" \
    --sql="$query" \
    --format=json | jq "{ \"_table\": \"$display_name\", rows: map(del(.flow_document)) }"
}

exportToJsonl "simple"
exportToJsonl "duplicate_keys_standard"
exportToJsonl "duplicate_keys_delta"
exportToJsonl "duplicate_keys_delta_exclude_flow_doc"
exportToJsonl '`duplicate keys @ with spaces`'
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
exportToJsonl "timezone_datetimes_delta"
