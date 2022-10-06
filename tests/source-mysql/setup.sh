#!/bin/bash

set -e
export TEST_STREAM="estuary_test_$(shuf -zer -n6 {a..z} | tr -d '\0')"
export RESOURCE="{ \"namespace\": \"test\", \"stream\": \"${TEST_STREAM}\" }"

export MYSQL_HOST="${MYSQL_HOST:=source-mysql-db-1.flow-test}"
export MYSQL_PORT="${MYSQL_PORT:=3306}"
export MYSQL_USER="${MYSQL_USER:=flow}"
export MYSQL_PASSWORD="${MYSQL_PASSWORD:=flow}"

docker compose -f source-mysql/docker-compose.yaml up --detach

config_json_template='{
    "address": "$MYSQL_HOST:$MYSQL_PORT",
    "user": "$MYSQL_USER",
    "password": "$MYSQL_PASSWORD"
}'

export CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"
echo "Connector configuration is: ${CONNECTOR_CONFIG}".

root_dir="$(git rev-parse --show-toplevel)"

function sql {
    echo "mysql> " $@
    echo "$@" | docker exec -i \
      source-mysql-db-1 \
      mysql $MYSQL_DATABASE --user=$MYSQL_USER --local-infile=1
}

sql "DROP TABLE IF EXISTS test.${TEST_STREAM};"
sql "CREATE TABLE test.${TEST_STREAM} (id INTEGER PRIMARY KEY, canary TEXT);"
sql "SET GLOBAL local_infile=1;"
sql "LOAD DATA LOCAL INFILE '/b.csv' INTO TABLE ${TEST_STREAM} FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;"
