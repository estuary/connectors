#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

function dropTable() {
    duckdb md: "drop table ${MOTHERDUCK_DATABASE}.${MOTHERDUCK_SCHEMA}."$1";"
}

echo "--- Running Cleanup ---"

# Remove materialized tables.
dropTable "simple_delta"
dropTable "duplicate_keys_delta"
dropTable "duplicate_keys_delta_exclude_flow_doc"
dropTable "multiple_types_delta"
dropTable "formatted_strings_delta"

# Remove the persisted materialization spec & checkpoint for this test materialization so subsequent
# runs start from scratch.
duckdb md: "delete from ${MOTHERDUCK_DATABASE}.${MOTHERDUCK_SCHEMA}.flow_checkpoints_v1 where materialization='tests/materialize-motherduck/materialize';"
duckdb md: "delete from ${MOTHERDUCK_DATABASE}.${MOTHERDUCK_SCHEMA}.flow_materializations_v2 where materialization='tests/materialize-motherduck/materialize';"
