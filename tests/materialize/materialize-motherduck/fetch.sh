#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

function exportToJsonl() {
  duckdb -json md: "set timezone to UTC; select * from ${MOTHERDUCK_DATABASE}.${MOTHERDUCK_SCHEMA}."$1" ORDER BY ID;" | jq "{ "_table": \"$1\", rows: . }"
}

exportToJsonl "simple_delta"
exportToJsonl "duplicate_keys_delta"
exportToJsonl "duplicate_keys_delta_exclude_flow_doc"
exportToJsonl "multiple_types_delta"
exportToJsonl "formatted_strings_delta"
