#!/bin/bash

set -e
export TEST_STREAM="estuary_test_$(shuf -zer -n6 {a..z} | tr -d '\0')"
export RESOURCE="{ \"namespace\": \"dbo\", \"stream\": \"${TEST_STREAM}\" }"

export CONTAINER_NAME="${CONTAINER_NAME:=source-sqlserver-db-1}"

export SQLSERVER_DATABASE=test
export SQLSERVER_HOST="${SQLSERVER_HOST:=${CONTAINER_NAME}.flow-test}"
export SQLSERVER_PORT="${SQLSERVER_PORT:=1433}"
export SQLSERVER_USER="${SQLSERVER_USER:=sa}"
export SQLSERVER_PASSWORD="${SQLSERVER_PASSWORD:=gf6w6dkD}"

docker compose -f source-sqlserver/docker-compose.yaml up --wait

config_json_template='{
    "address": "$SQLSERVER_HOST:$SQLSERVER_PORT",
    "user": "$SQLSERVER_USER",
    "password": "$SQLSERVER_PASSWORD",
    "database": "$SQLSERVER_DATABASE"
}'

export CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"
echo "Connector configuration is: ${CONNECTOR_CONFIG}".

root_dir="$(git rev-parse --show-toplevel)"

function sql {
    echo "sql> " $@
    echo -e "$@\nGO\n" | docker exec -i \
      ${CONTAINER_NAME} \
      /opt/mssql-tools/bin/sqlcmd \
      -U ${SQLSERVER_USER} \
      -P ${SQLSERVER_PASSWORD}
}

sql "
USE test;
GO
DROP TABLE IF EXISTS dbo.${TEST_STREAM};
GO
CREATE TABLE dbo.${TEST_STREAM} (id INTEGER PRIMARY KEY NOT NULL, canary TEXT);
GO
INSERT INTO dbo.${TEST_STREAM} VALUES
  (11, \"amputation's\"),
  (12, \"armament's\"),
  (13, \"splatters\"),
  (14, \"strengthen\"),
  (15, \"Kringle's\"),
  (16, \"grosbeak's\"),
  (17, \"pieced\"),
  (18, \"roaches\"),
  (19, \"devilish\"),
  (20, \"glucose's\");
GO
"