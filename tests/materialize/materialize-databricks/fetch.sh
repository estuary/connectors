#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

function exportToJsonl() {
  go run ${TEST_DIR}/materialize-databricks/fetch-data.go "$1" | jq "{ \"table\": \"$1\", rows: . }"
}

exportToJsonl "\`$DATABRICKS_CATALOG\`.\`some-schema\`.simple"
exportToJsonl "\`$DATABRICKS_CATALOG\`.\`some-schema\`.duplicate_keys_standard"
exportToJsonl "\`$DATABRICKS_CATALOG\`.\`some-schema\`.duplicate_keys_delta"
exportToJsonl "\`$DATABRICKS_CATALOG\`.\`some-schema\`.duplicate_keys_delta_exclude_flow_doc"
exportToJsonl "\`$DATABRICKS_CATALOG\`.\`some-schema\`.multiple_types"
exportToJsonl "\`$DATABRICKS_CATALOG\`.\`some-schema\`.formatted_strings"
