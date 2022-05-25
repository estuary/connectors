#!/bin/bash

set -e
export TEST_STREAM="estuary_test_$(shuf -zer -n6 {a..z} | tr -d '\0')"
export RESOURCE="{ \"namespace\": \"test\", \"stream\": \"${TEST_STREAM}\" }"

config_json_template='{
    "address": "$MYSQL_HOST:$MYSQL_PORT",
    "login": {
      "user": "$MYSQL_USER",
      "password": "$MYSQL_PWD"
    }
}'

export CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"
echo "Connector configuration is: ${CONNECTOR_CONFIG}".

root_dir="$(git rev-parse --show-toplevel)"

function sql {
    echo "mysql> " $@
    echo "$@" | mysql $MYSQL_DATABASE --user=$MYSQL_USER --local-infile=1
}

sql "DROP TABLE IF EXISTS test.${TEST_STREAM};"
sql "CREATE TABLE test.${TEST_STREAM} (id INTEGER PRIMARY KEY, canary TEXT);"
sql "SET GLOBAL local_infile=1;"
sql "LOAD DATA LOCAL INFILE '${root_dir}/tests/files/b.csv' INTO TABLE ${TEST_STREAM} FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;"
