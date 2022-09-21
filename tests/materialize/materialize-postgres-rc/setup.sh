#!/bin/bash

set -e

# Ensure canonical Postgres environment variables are set.
export PGHOST="${PGHOST:=localhost}"
export PGPORT="${PGPORT:=5432}"
export PGDATABASE="${PGDATABASE:=postgres}"
export PGPASSWORD="${PGPASSWORD:=postgres}"
export PGUSER="${PGUSER:=postgres}"


config_json_template='{
   "address":  "$PGHOST:$PGPORT",
   "database": "$PGDATABASE",
   "password": "$PGPASSWORD",
   "user":     "$PGUSER",
   "schema":   "public"
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
  }
]'

export CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"
echo "Connector configuration is: ${CONNECTOR_CONFIG}".

export RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"
echo "Resources configuration is: ${RESOURCES_CONFIG}".