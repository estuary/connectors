#!/bin/bash

set -o pipefail
set -o nounset

function dropTable() {
    duckdb md: "drop table ${MOTHERDUCK_DATABASE}.${MOTHERDUCK_SCHEMA}."$1";" || true
}

echo "--- Running Cleanup ---"

# Remove materialized tables.
dropTable "simple"
dropTable "duplicate_keys_standard"
dropTable "duplicate_keys_delta"
dropTable "duplicate_keys_delta_exclude_flow_doc"
dropTable "multiple_types"
dropTable "formatted_strings"
dropTable "unsigned_bigint"
dropTable "deletions"

# Remove the persisted materialization spec & checkpoint for this test materialization so subsequent
# runs start from scratch.
duckdb md: "delete from ${MOTHERDUCK_DATABASE}.${MOTHERDUCK_SCHEMA}.flow_checkpoints_v1 where materialization='tests/materialize-motherduck/materialize';"
