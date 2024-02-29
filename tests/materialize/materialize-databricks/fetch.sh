#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

function exportToJsonl() {
	dbsqlcli -e "SELECT * FROM $1;" | mlr --icsv --ojsonl cat | jq -c "{ table: \"$1\", row: . }"
}

exportToJsonl "\`$DATABRICKS_CATALOG\`.\`some-schema\`.simple"
exportToJsonl "\`$DATABRICKS_CATALOG\`.\`some-schema\`.duplicate_keys_standard"
exportToJsonl "\`$DATABRICKS_CATALOG\`.\`some-schema\`.duplicate_keys_delta"
exportToJsonl "\`$DATABRICKS_CATALOG\`.\`some-schema\`.duplicate_keys_delta_exclude_flow_doc"
exportToJsonl "\`$DATABRICKS_CATALOG\`.\`some-schema\`.multiple_types"

# due to a bug in the cli, we can't use this at the moment,
# see https://github.com/databricks/databricks-sql-cli/issues/51
# exportToJsonl "\`$DATABRICKS_CATALOG\`.default.formatted_strings"
dbsqlcli -e "SELECT id, datetime::string, date::string, flow_published_at, int_and_str, int_str, num_and_str, num_str, time, flow_document FROM main.\`some-schema\`.formatted_strings;" | \
	mlr --icsv --ojsonl cat | jq -c "del(.flow_document) | { table: \"\`$DATABRICKS_CATALOG\`.\`some-schema\`.formatted_strings\", row: . }"
