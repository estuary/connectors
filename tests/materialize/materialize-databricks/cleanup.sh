#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

function dropTable() {
	echo "y" | dbsqlcli -e "DROP TABLE IF EXISTS $1";
}

# Remove materialized tables.
dropTable "\`$DATABRICKS_CATALOG\`.default.simple"
dropTable "\`$DATABRICKS_CATALOG\`.default.duplicate_keys_standard"
dropTable "\`$DATABRICKS_CATALOG\`.default.duplicate_keys_delta"
dropTable "\`$DATABRICKS_CATALOG\`.default.duplicate_keys_delta_exclude_flow_doc"
dropTable "\`$DATABRICKS_CATALOG\`.default.multiple_types"
dropTable "\`$DATABRICKS_CATALOG\`.default.formatted_strings"

yes | dbsqlcli -e "delete from \`$DATABRICKS_CATALOG\`.\`default\`.flow_materializations_v2 where MATERIALIZATION='tests/materialize-databricks/materialize';"

