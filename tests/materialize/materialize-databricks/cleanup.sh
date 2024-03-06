#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

function dropTable() {
	echo "dropping $1"
	echo "y" | dbsqlcli -e "DROP TABLE IF EXISTS $1";
}

# Remove materialized tables.
dropTable "\`$DATABRICKS_CATALOG\`.\`some-schema\`.simple"
dropTable "\`$DATABRICKS_CATALOG\`.\`some-schema\`.duplicate_keys_standard"
dropTable "\`$DATABRICKS_CATALOG\`.\`some-schema\`.duplicate_keys_delta"
dropTable "\`$DATABRICKS_CATALOG\`.\`some-schema\`.duplicate_keys_delta_exclude_flow_doc"
dropTable "\`$DATABRICKS_CATALOG\`.\`some-schema\`.multiple_types"
dropTable "\`$DATABRICKS_CATALOG\`.\`some-schema\`.formatted_strings"

yes | dbsqlcli -e "delete from \`$DATABRICKS_CATALOG\`.\`some-schema\`.flow_materializations_v2 where MATERIALIZATION='tests/materialize-databricks/materialize';"

