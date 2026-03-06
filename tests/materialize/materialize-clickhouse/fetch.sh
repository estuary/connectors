#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

# Standard tables use ReplacingMergeTree with _version and _is_deleted columns.
# FINAL deduplicates at query time, and we filter out soft-deleted rows.
function exportStandard() {
  query "SELECT * EXCEPT (_version, _is_deleted) FROM \`$1\` FINAL WHERE _is_deleted = 0 ORDER BY flow_published_at, id FORMAT JSON;" \
    | jq -c "{ table: \"$1\", rows: .data | map(del(.flow_document)) }"
}

# Delta tables use MergeTree without _version or _is_deleted columns.
function exportDelta() {
  query "SELECT * FROM \`$1\` ORDER BY flow_published_at, id FORMAT JSON;" \
    | jq -c "{ table: \"$1\", rows: .data | map(del(.flow_document)) }"
}

exportStandard "simple"
exportStandard "duplicate_keys_standard"
exportDelta "duplicate_keys_delta"
exportDelta "duplicate_keys_delta_exclude_flow_doc"
exportStandard "multiple_types"
exportStandard "formatted_strings"
exportStandard "long_string"
exportStandard "unsigned_bigint"
exportStandard "deletions"
exportStandard "all_key_types_part_one"
exportStandard "all_key_types_part_two"
exportStandard "all_key_types_part_three"
exportStandard "fields_with_projections"
exportStandard "many_columns"
exportStandard "timezone_datetimes_standard"
exportDelta "timezone_datetimes_delta"
