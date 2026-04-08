#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

# Standard tables use ReplacingMergeTree with _is_deleted for deduplication and hard deletes.
# FINAL filters duplicate and deleted rows at query time.
function exportTable() {
  query "SELECT * EXCEPT (_is_deleted) FROM \`$1\` FINAL ORDER BY flow_published_at, id FORMAT JSON;" \
    | jq -c "{ table: \"$1\", rows: .data | map(del(.flow_document)) }"
}

# Delta-update tables use plain MergeTree and have no _is_deleted column.
function exportDeltaTable() {
  query "SELECT * FROM \`$1\` ORDER BY flow_published_at FORMAT JSON;" \
    | jq -c "{ table: \"$1\", rows: .data | map(del(.flow_document)) }"
}

exportTable "simple"
exportTable "duplicate_keys_standard"
exportDeltaTable "duplicate_keys_delta"
exportDeltaTable "duplicate_keys_delta_exclude_flow_doc"
exportTable "multiple_types"
exportTable "formatted_strings"
exportTable "long_string"
exportTable "unsigned_bigint"
exportTable "deletions"
exportTable "all_key_types_part_one"
exportTable "all_key_types_part_two"
exportTable "all_key_types_part_three"
exportTable "fields_with_projections"
exportTable "many_columns"
exportTable "timezone_datetimes_standard"
