#!/bin/bash

set -e
export TEST_STREAM="estuary_test_$(shuf -zer -n6 {a..z} | tr -d '\0')"
export RESOURCE="{ stream: ${TEST_STREAM} }"


if [[ "${SSH_FORWARDING_ENABLED}" == "true" ]]
then
    if [[ ! -f "${PRIVATE_KEY_FILE_PATH}" ]]; then
        bail "${PRIVATE_KEY_FILE_PATH} does not exists."
    fi
    export PRIVATE_KEY="$(awk '{printf "%s\\n", $0}' ${PRIVATE_KEY_FILE_PATH})"

    config_json_template='{
        "address": "localhost:$LOCALPORT",
        "user": "$MYSQL_USER",
        "password": "$MYSQL_PWD",
        "dbname": "$MYSQL_DATABASE",
        "server_id": $MYSQL_SERVERID,
        "networkProxy": {
            "sshForwarding": {
              "sshEndpoint": "$SSHENDPOINT",
              "user": "$SSHUSER",
              "forwardHost": "$MYSQL_HOST",
              "forwardPort": $MYSQL_PORT,
              "privateKey": "$PRIVATE_KEY",
              "localPort": $LOCALPORT
            }
        }
    }'
else
    config_json_template='{
        "address": "$MYSQL_HOST:$MYSQL_PORT",
        "user": "$MYSQL_USER",
        "password": "$MYSQL_PWD",
        "dbname": "$MYSQL_DATABASE",
        "server_id": $MYSQL_SERVERID
    }'
fi

echo "$(echo "$config_json_template" | envsubst)"
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
