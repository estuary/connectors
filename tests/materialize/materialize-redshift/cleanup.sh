#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

function dropTable() {
    echo "drop table if exists \"$1\";" | psql postgres://${REDSHIFT_USER}:${REDSHIFT_PASSWORD}@${REDSHIFT_ADDRESS}/${REDSHIFT_DATABASE}
}

echo "--- Running Cleanup ---"

# Remove materialized tables.
dropTable "simple"
dropTable "duplicate_keys_standard"
dropTable "duplicate_keys_delta"
dropTable "duplicate_keys_delta_exclude_flow_doc"
dropTable "multiple_types"
dropTable "formatted_strings"
dropTable "long_string"

# Remove the persisted materialization spec & checkpoint for this test materialization so subsequent
# runs start from scratch.
echo "delete from FLOW_CHECKPOINTS_V1 where MATERIALIZATION='tests/materialize-redshift/materialize';" | psql postgres://${REDSHIFT_USER}:${REDSHIFT_PASSWORD}@${REDSHIFT_ADDRESS}/${REDSHIFT_DATABASE}
echo "delete from FLOW_MATERIALIZATIONS_V2 where MATERIALIZATION='tests/materialize-redshift/materialize';" | psql postgres://${REDSHIFT_USER}:${REDSHIFT_PASSWORD}@${REDSHIFT_ADDRESS}/${REDSHIFT_DATABASE}
