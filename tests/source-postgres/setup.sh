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
        "database": "$PGDATABASE",
        "host":     "localhost",
        "port":     $LOCALPORT,
        "password": "PGPASSWORD",
        "user":     "$PGUSER",
        "networkTunnel": {
            "tunnelType": "sshForwarding",
            "sshForwarding": {
              "sshEndpoint": "$SSHENDPOINT",
              "user": "$SSHUSER",
              "forwardHost": "$PGHOST",
              "forwardPort": $PGPORT,
              "privateKey": "$PRIVATE_KEY",
              "localPort": $LOCALPORT
            }
        }
    }'
else
    config_json_template='{
       "database": "$PGDATABASE",
       "host":     "$PGHOST",
       "password": "$PGPASSWORD",
       "port":     $PGPORT,
       "user":     "$PGUSER"
     }'
fi

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
