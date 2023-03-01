#!/bin/bash

set -e
export PGHOST="${PGHOST:=source-postgres-db-1.flow-test}"
export PGPORT="${PGPORT:=5432}"
export PGDATABASE="${PGDATABASE:=postgres}"
export PGPASSWORD="${PGPASSWORD:=postgres}"
export PGUSER="${PGUSER:=postgres}"

export TEST_STREAM="estuary_test_$(shuf -zer -n6 {a..z} | tr -d '\0')"
export RESOURCE="{ \"namespace\": \"public\", \"stream\": \"${TEST_STREAM}\" }"

docker compose -f source-postgres/docker-compose.yaml up --detach

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

    docker exec \
      -e PGUSER=$PGUSER \
      -e PGPASSWORD=$PGPASSWORD \
      -e PGDATABASE=$PGDATABASE \
      source-postgres-db-1 \
      psql -c "$@"
}

sql "DROP TABLE IF EXISTS ${TEST_STREAM};"
sql "CREATE TABLE ${TEST_STREAM} (id INTEGER PRIMARY KEY NOT NULL, canary TEXT);"
sql "\\copy ${TEST_STREAM} FROM '/b.csv' DELIMITER ',' CSV HEADER;"
