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
      "table": "long_string"
    },
    "source": "${TEST_COLLECTION_LONG_STRING}"
  },
  {
    "resource": {
      "table": "deletions"
    },
    "source": "${TEST_COLLECTION_DELETIONS}"
  }
]'

export CONNECTOR_CONFIG="$(decrypt_config ${TEST_DIR}/${CONNECTOR}/config.yaml)"
export REDSHIFT_ADDRESS="$(echo $CONNECTOR_CONFIG | jq -r .address)"
export REDSHIFT_USER="$(echo $CONNECTOR_CONFIG | jq -r .user)"
export REDSHIFT_PASSWORD="$(echo $CONNECTOR_CONFIG | jq -r .password)"
export REDSHIFT_DATABASE="$(echo $CONNECTOR_CONFIG | jq -r .database)"
export REDSHIFT_BUCKET="$(echo $CONNECTOR_CONFIG | jq -r .bucket)"
export AWS_ACCESS_KEY_ID="$(echo $CONNECTOR_CONFIG | jq -r .awsAccessKeyId)"
export AWS_SECRET_ACCESS_KEY="$(echo $CONNECTOR_CONFIG | jq -r .awsSecretAccessKey)"
export AWS_REGION="$(echo $CONNECTOR_CONFIG | jq -r .region)"

export RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"
