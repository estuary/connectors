#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

function dropTable() {
	echo "dropping $1"
	go run ${TEST_DIR}/materialize-databricks/fetch-data.go --run-query "DROP TABLE IF EXISTS $1"
}

# Remove materialized tables.
dropTable "\`$DATABRICKS_CATALOG\`.\`some-schema\`.simple"
dropTable "\`$DATABRICKS_CATALOG\`.\`some-schema\`.duplicate_keys_standard"
dropTable "\`$DATABRICKS_CATALOG\`.\`some-schema\`.duplicate_keys_delta"
dropTable "\`$DATABRICKS_CATALOG\`.\`some-schema\`.duplicate_keys_delta_exclude_flow_doc"
dropTable "\`$DATABRICKS_CATALOG\`.\`some-schema\`.multiple_types"
dropTable "\`$DATABRICKS_CATALOG\`.\`some-schema\`.formatted_strings"
dropTable "\`$DATABRICKS_CATALOG\`.\`some-schema\`.unsigned_bigint"
dropTable "\`$DATABRICKS_CATALOG\`.\`some-schema\`.deletions"
dropTable "\`$DATABRICKS_CATALOG\`.\`some-schema\`.binary_key"
dropTable "\`$DATABRICKS_CATALOG\`.\`some-schema\`.all_key_types_part_one"
dropTable "\`$DATABRICKS_CATALOG\`.\`some-schema\`.all_key_types_part_two"
dropTable "\`$DATABRICKS_CATALOG\`.\`some-schema\`.all_key_types_part_three"
dropTable "\`$DATABRICKS_CATALOG\`.\`some-schema\`.fields_with_projections"
dropTable "\`$DATABRICKS_CATALOG\`.\`some-schema\`.many_columns"
