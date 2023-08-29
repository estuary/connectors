#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

# Ensure canonical mysql environment variables are set.
export MYSQL_HOST="${MYSQL_HOST:=materialize-mysql-mysql-1.flow-test}"
export MYSQL_PORT="${MYSQL_PORT:=3306}"
export MYSQL_DATABASE="${MYSQL_DATABASE:=flow}"
export MYSQL_PASSWORD="${MYSQL_PASSWORD:=flow}"
export MYSQL_USER="${MYSQL_USER:=flow}"

docker compose -f materialize-mysql/docker-compose.yaml up --wait

function query() {
  echo "$1" | docker exec -i \
    materialize-mysql-mysql-1 mysqlsh --host=127.0.0.1 --port=$MYSQL_PORT --user=root --password=$MYSQL_PASSWORD --database $MYSQL_DATABASE --sql --json=raw
}

query "GRANT ALL PRIVILEGES ON *.* TO 'flow'@'%' WITH GRANT OPTION"
# Older version of MySQL handle tables with multiple timestamp columns in a very strange way with
# respect to nullability and default values, and this causes issues when materializing a table with
# more than one date-time format field. Setting this flag allows tables with multiple timestamp
# columns to be created. It may be worth addressing this at a more hollistic level in the
# connector's table generation templating. See:
# https://dba.stackexchange.com/questions/314898/why-mysql-5-7-timestamp-not-null-requires-default-value
query "SET GLOBAL explicit_defaults_for_timestamp = 1;"

config_json_template='{
   "address":  "$MYSQL_HOST:$MYSQL_PORT",
   "database": "$MYSQL_DATABASE",
   "password": "$MYSQL_PASSWORD",
   "user":     "$MYSQL_USER"
}'

resources_json_template='[
  {
    "resource": {
      "table": "Simple"
    },
    "source": "${TEST_COLLECTION_SIMPLE}"
  },
  {
    "resource": {
      "table": "duplicate_keys_standard"
    },
    "source": "${TEST_COLLECTION_DUPLICATED_KEYS}"
  },
  {
    "resource": {
      "table": "duplicate_keys_delta",
      "delta_updates": true
    },
    "source": "${TEST_COLLECTION_DUPLICATED_KEYS}"
  },
  {
    "resource": {
      "table": "duplicate_keys_delta_exclude_flow_doc",
      "delta_updates": true
    },
    "source": "${TEST_COLLECTION_DUPLICATED_KEYS}",
    "fields": {
      "recommended": true,
      "exclude": [
        "flow_document" 
      ]
    }
  },
  {
    "resource": {
      "table": "Multiple Types"
    },
    "source": "${TEST_COLLECTION_MULTIPLE_DATATYPES}",
    "fields": {
      "recommended": true,
      "exclude": ["nested/id"],
      "include": {
        "nested": {},
        "array_int": {}
      }
    }
  },
  {
    "resource": {
      "table": "Formatted Strings"
    },
    "source": "${TEST_COLLECTION_FORMATTED_STRINGS}",
    "fields": {
      "recommended": true
    }
  },
  {
    "resource": {
      "table": "long-string"
    },
    "source": "${TEST_COLLECTION_LONG_STRING}"
  }
]'

export CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"
export RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"
