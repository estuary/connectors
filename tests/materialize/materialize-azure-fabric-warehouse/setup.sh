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
      "table": "deletions"
    },
    "source": "${TEST_COLLECTION_DELETIONS}"
  },
  {
    "resource": {
      "table": "binary_key"
    },
    "source": "${TEST_COLLECTION_BINARY_KEY}"
  },
  {
    "resource": {
      "table": "string_escaped_key"
    },
    "source": "${TEST_COLLECTION_STRING_ESCAPED_KEY}"
  },
  {
    "resource": {
      "table": "all_key_types_part_one"
    },
    "source": "${TEST_COLLECTION_ALL_KEY_TYPES_PART_ONE}"
  },
  {
    "resource": {
      "table": "all_key_types_part_two"
    },
    "source": "${TEST_COLLECTION_ALL_KEY_TYPES_PART_TWO}"
  },
  {
    "resource": {
      "table": "all_key_types_part_three"
    },
    "source": "${TEST_COLLECTION_ALL_KEY_TYPES_PART_THREE}"
  }
]'

export CONNECTOR_CONFIG="$(decrypt_config ${TEST_DIR}/${CONNECTOR}/config.yaml)"
export FABRIC_WAREHOUSE_CLIENT_ID="$(echo $CONNECTOR_CONFIG | jq -r .clientID)"
export FABRIC_WAREHOUSE_CLIENT_SECRET="$(echo $CONNECTOR_CONFIG | jq -r .clientSecret)"
export FABRIC_WAREHOUSE_CONNECTION_STRING="$(echo $CONNECTOR_CONFIG | jq -r .connectionString)"
export FABRIC_WAREHOUSE_WAREHOUSE="$(echo $CONNECTOR_CONFIG | jq -r .warehouse)"
export FABRIC_WAREHOUSE_SCHEMA="$(echo $CONNECTOR_CONFIG | jq -r .schema)"

export RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"
