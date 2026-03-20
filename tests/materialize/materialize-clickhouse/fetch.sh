#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

# All tables use ReplacingMergeTree to deduplicate and hard-delete delete rows asynchronously.
# FINAL filters duplicate and deleted rows at query time.
function exportTable() {
  query "SELECT * EXCEPT (_is_deleted) FROM \`$1\` FINAL ORDER BY flow_published_at, id FORMAT JSON;" \
    | jq -c "{ table: \"$1\", rows: .data | map(del(.flow_document)) }"
}

exportTable "simple"
exportTable "duplicate_keys_standard"
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
