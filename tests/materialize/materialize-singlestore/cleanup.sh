#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

function dropTable() {
	echo "dropping $1"
	query "DROP TABLE IF EXISTS \`$1\`;"
}

# Remove materialized tables.
dropTable "Simple"
dropTable "duplicate_keys_standard"
dropTable "duplicate_keys_delta"
dropTable "duplicate_keys_delta_exclude_flow_doc"
dropTable "Multiple Types"
dropTable "Formatted Strings"
dropTable "long-string"
dropTable "unsigned_bigint"
dropTable "deletions"
dropTable "all_key_types_part_one"
dropTable "all_key_types_part_two"
dropTable "all_key_types_part_three"
dropTable "fields_with_projections"
dropTable "many_columns"
