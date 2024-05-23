#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

function exportToJsonl() {
  duckdb md: "set timezone to UTC; copy(select * from ${MOTHERDUCK_DATABASE}.${MOTHERDUCK_SCHEMA}."$1" order by ID) to '/dev/stdout' (format json);" | jq "{ "_table": \"$1\", rows: . }"
}

exportToJsonl "simple_delta"
exportToJsonl "duplicate_keys_delta"
exportToJsonl "duplicate_keys_delta_exclude_flow_doc"
exportToJsonl "multiple_types_delta"
exportToJsonl "formatted_strings_delta"
