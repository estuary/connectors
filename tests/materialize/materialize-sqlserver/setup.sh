#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

# Ensure canonical mysql environment variables are set.
export SQLSERVER_HOST="${SQLSERVER_HOST:=materialize-sqlserver-sqlserver-1}"
export SQLSERVER_PORT="${SQLSERVER_PORT:=1433}"
export SQLSERVER_DATABASE="${SQLSERVER_DATABASE:=flow}"
export SQLSERVER_PASSWORD="${SQLSERVER_PASSWORD:=!Flow1234}"
export SQLSERVER_USER="${SQLSERVER_USER:=sa}"

docker compose -f materialize-sqlserver/docker-compose.yaml up --wait

docker build -t sqlserver-test-query:local -f tests/materialize/materialize-sqlserver/Dockerfile tests/materialize/materialize-sqlserver/

function query() {
  echo "$1" | docker run -e SQLSERVER_HOST -e SQLSERVER_PORT \
		-e SQLSERVER_DATABASE -e SQLSERVER_USER \
		-e SQLSERVER_PASSWORD --network flow-test \
		-i sqlserver-test-query:local
}

SQLSERVER_DATABASE=master query "IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '$SQLSERVER_DATABASE')
BEGIN
  CREATE DATABASE $SQLSERVER_DATABASE;
END;"

config_json_template='{
   "address":  "$SQLSERVER_HOST:$SQLSERVER_PORT",
   "database": "$SQLSERVER_DATABASE",
   "password": "$SQLSERVER_PASSWORD",
   "user":     "$SQLSERVER_USER"
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
        "array_int": {},
        "multiple": {}
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
  }
]'

export CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"
export RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"
