#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

export SNOWFLAKE_HOST="${SNOWFLAKE_HOST}"
export SNOWFLAKE_ACCOUNT="${SNOWFLAKE_ACCOUNT}"
export SNOWFLAKE_USER="${SNOWFLAKE_USER}"
export SNOWFLAKE_PASSWORD="${SNOWFLAKE_PASSWORD}"
export SNOWFLAKE_DATABASE="${SNOWFLAKE_DATABASE}"
export SNOWFLAKE_SCHEMA="${SNOWFLAKE_SCHEMA}"
export SNOWFLAKE_WAREHOUSE="${SNOWFLAKE_WAREHOUSE}"

config_json_template='{
   "host":      "$SNOWFLAKE_HOST",
   "account":   "$SNOWFLAKE_ACCOUNT",
   "user":      "$SNOWFLAKE_USER",
   "password":  "$SNOWFLAKE_PASSWORD",
   "database":  "$SNOWFLAKE_DATABASE",
   "schema":    "$SNOWFLAKE_SCHEMA",
   "warehouse": "$SNOWFLAKE_WAREHOUSE"
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
