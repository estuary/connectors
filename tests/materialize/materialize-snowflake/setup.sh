#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

export SNOWFLAKE_HOST="${SNOWFLAKE_HOST}"
export SNOWFLAKE_ACCOUNT="${SNOWFLAKE_ACCOUNT}"
export SNOWFLAKE_DATABASE="${SNOWFLAKE_DATABASE}"
export SNOWFLAKE_SCHEMA="${SNOWFLAKE_SCHEMA}"
export SNOWFLAKE_WAREHOUSE="${SNOWFLAKE_WAREHOUSE}"

export SNOWFLAKE_AUTH_TYPE="${SNOWFLAKE_AUTH_TYPE}"
# if auth type is user_password
export SNOWFLAKE_USER="${SNOWFLAKE_USER:-}"
export SNOWFLAKE_PASSWORD="${SNOWFLAKE_PASSWORD:-}"
# if auth type is jwt
export SNOWFLAKE_PRIVATE_KEY="$(cat ${SNOWFLAKE_PRIVATE_KEY:-} | jq -sR . | sed -e 's/^"//' -e 's/"$//')"

config_json_template='{
   "host":      "$SNOWFLAKE_HOST",
   "account":   "$SNOWFLAKE_ACCOUNT",
   "database":  "$SNOWFLAKE_DATABASE",
   "schema":    "$SNOWFLAKE_SCHEMA",
   "warehouse": "$SNOWFLAKE_WAREHOUSE",
   "credentials": {
     "auth_type": "$SNOWFLAKE_AUTH_TYPE",
     "user": "$SNOWFLAKE_USER",
     "password": "$SNOWFLAKE_PASSWORD",
     "private_key": "$SNOWFLAKE_PRIVATE_KEY"
   },
   "advanced": {
      "updateDelay": "0s"
    }
}'

resources_json_template='[
  {
    "resource": {
      "table": "simple"
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
      "table": "multiple_types"
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
      "table": "formatted_strings"
    },
    "source": "${TEST_COLLECTION_FORMATTED_STRINGS}",
    "fields": {
      "recommended": true
    }
  },
  {
    "resource": {
      "table": "symbols"
    },
    "source": "${TEST_COLLECTION_SYMBOLS}",
    "fields": {
      "recommended": true
    }
  }
]'

export CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"
export RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"
