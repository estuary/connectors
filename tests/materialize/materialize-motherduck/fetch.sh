#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

function exportToJsonl() {
  duckdb md: "set timezone to UTC; copy(select * from ${MOTHERDUCK_DATABASE}.${MOTHERDUCK_SCHEMA}."$1" order by ID) to '/dev/stdout' (format json);" | jq "{ "_table": \"$1\", rows: . }"
}

exportToJsonl "simple"
exportToJsonl "duplicate_keys_standard"
exportToJsonl "duplicate_keys_delta"
exportToJsonl "duplicate_keys_delta_exclude_flow_doc"
exportToJsonl "multiple_types"
exportToJsonl "formatted_strings"
exportToJsonl "unsigned_bigint"
exportToJsonl "deletions"
exportToJsonl "all_key_types_part_one"
exportToJsonl "all_key_types_part_two"
exportToJsonl "all_key_types_part_three"
