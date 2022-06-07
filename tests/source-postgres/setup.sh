#!/bin/bash

set -e
export TEST_STREAM="estuary_test_$(shuf -zer -n6 {a..z} | tr -d '\0')"
export RESOURCE="{ stream: ${TEST_STREAM} }"

config_json_template='{
   "address":  "$PGHOST:$PGPORT",
   "database": "$PGDATABASE",
   "password": "$PGPASSWORD",
   "user":     "$PGUSER"
}'

export CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"
echo "Connector configuration is: ${CONNECTOR_CONFIG}".

root_dir="$(git rev-parse --show-toplevel)"

function sql {
    echo "psql> " $@
    psql -c "$@"
}

sql "DROP TABLE IF EXISTS ${TEST_STREAM};"
sql "CREATE TABLE ${TEST_STREAM} (id INTEGER PRIMARY KEY, canary TEXT);"
sql "\\copy ${TEST_STREAM} FROM '${root_dir}/tests/files/b.csv' DELIMITER ',' CSV HEADER;"
