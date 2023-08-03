#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

# Ensure canonical Postgres environment variables are set.
export MYSQL_HOST="${MYSQL_HOST:=materialize-mysql-mysql-1.flow-test}"
export MYSQL_PORT="${MYSQL_PORT:=3306}"
export MYSQL_DATABASE="${MYSQL_DATABASE:=flow}"
export MYSQL_PASSWORD="${MYSQL_PASSWORD:=flow}"
export MYSQL_USER="${MYSQL_USER:=flow}"

docker compose -f materialize-mysql/docker-compose.yaml up --detach
# Give it time to start.
sleep 10

function query() {
  echo "$1" | docker exec -i \
    materialize-mysql-mysql-1 mysqlsh --host=127.0.0.1 --port=$MYSQL_PORT --user=root --password=$MYSQL_PASSWORD --database $MYSQL_DATABASE --sql --json=raw
}

query "GRANT ALL PRIVILEGES ON *.* TO 'flow'@'%' WITH GRANT OPTION"

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
      "recommended": true,
      "include": {
        "int_and_str": {},
        "num_and_str": {},
        "int_str": {},
        "num_str": {}
      }
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
