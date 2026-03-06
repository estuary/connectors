#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

# All tables use ReplacingMergeTree with _version and _is_deleted columns.
# FINAL deduplicates at query time, and we filter out soft-deleted rows.
function exportTable() {
  query "SELECT * EXCEPT (_version, _is_deleted) FROM \`$1\` FINAL WHERE _is_deleted = 0 ORDER BY flow_published_at, id FORMAT JSON;" \
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
