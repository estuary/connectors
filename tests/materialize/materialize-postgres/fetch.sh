#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

function exportToJsonl() {
  docker exec \
    -e PGUSER=$PGUSER \
    -e PGPASSWORD=$PGPASSWORD \
    -e PGDATABASE=$PGDATABASE \
    materialize-postgres-postgres-1 psql \
    --tuples-only \
    --command "select row_to_json(t) from \"$1\" as t;" |
      jq -c -s "{ table: \"$1\", rows: map(del(.flow_document)) }"
}

exportToJsonl "Simple"
exportToJsonl "duplicate_keys_standard"
exportToJsonl "duplicate_keys_delta"
exportToJsonl "duplicate_keys_delta_exclude_flow_doc"
exportToJsonl "Multiple Types"
exportToJsonl "Formatted Strings"
exportToJsonl "unsigned_bigint"
exportToJsonl "deletions"
