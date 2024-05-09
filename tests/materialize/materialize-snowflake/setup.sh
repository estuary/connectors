#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

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
  },
  {
    "resource": {
      "table": "unsigned_bigint"
    },
    "source": "${TEST_COLLECTION_UNSIGNED_BIGINT}"
  }
]'

export CONNECTOR_CONFIG="$(decrypt_config ${TEST_DIR}/${CONNECTOR}/config.yaml)"
export SNOWFLAKE_HOST="$(echo $CONNECTOR_CONFIG | jq -r .host)"
export SNOWFLAKE_ACCOUNT="$(echo $CONNECTOR_CONFIG | jq -r .account)"
export SNOWFLAKE_DATABASE="$(echo $CONNECTOR_CONFIG | jq -r .database)"
export SNOWFLAKE_SCHEMA="$(echo $CONNECTOR_CONFIG | jq -r .schema)"
export SNOWFLAKE_WAREHOUSE="$(echo $CONNECTOR_CONFIG | jq -r .warehouse)"

export SNOWFLAKE_AUTH_TYPE="$(echo $CONNECTOR_CONFIG | jq -r .credentials.auth_type)"
# if auth type is user_password
export SNOWFLAKE_USER="$(echo $CONNECTOR_CONFIG | jq -r .credentials.user)"
if [ "$SNOWFLAKE_AUTH_TYPE" == "jwt" ]; then 
  # if auth type is jwt
  export SNOWFLAKE_PRIVATE_KEY="$(echo $CONNECTOR_CONFIG | jq -r .credentials.private_key)"
else
  export SNOWFLAKE_PASSWORD="$(echo $CONNECTOR_CONFIG | jq -r .credentials.password)"
fi

export RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"
