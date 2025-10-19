#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset


resources_json_template='[
  {
    "resource": {
      "table": "simple",
      "table_type": "fact"
    },
    "fields": {
      "recommended": true
    },
    "source": "${TEST_COLLECTION_SIMPLE}"
  },
  {
    "resource": {
      "table": "duplicate_keys_standard",
      "table_type": "fact"
    },
    "fields": {
      "recommended": true
    },
    "source": "${TEST_COLLECTION_DUPLICATED_KEYS}"
  },
  {
    "resource": {
      "table": "formatted_strings",
      "table_type": "fact"
    },
    "source": "${TEST_COLLECTION_FORMATTED_STRINGS}",
    "fields": {
      "recommended": true
    }
  },
  {
    "resource": {
      "table": "long_string",
      "table_type": "fact"
    },
    "fields": {
      "recommended": true
    },
    "source": "${TEST_COLLECTION_LONG_STRING}"
  },
  {
    "resource": {
      "table": "deletions",
      "table_type": "fact"
    },
    "fields": {
      "recommended": true
    },
    "source": "${TEST_COLLECTION_DELETIONS}"
  }
]'


export CONNECTOR_CONFIG="$(decrypt_config $CONNECTOR_TEST_DIR/config.yaml)"
export FIREBOLT_ACCOUNT="$(echo $CONNECTOR_CONFIG | jq -r .account_name)"
export FIREBOLT_CLIENT_ID="$(echo $CONNECTOR_CONFIG | jq -r .client_id)"
export FIREBOLT_CLIENT_SECRET="$(echo $CONNECTOR_CONFIG | jq -r .client_secret)"
export FIREBOLT_DATABASE="$(echo $CONNECTOR_CONFIG | jq -r .database)"
export FIREBOLT_ENGINE="$(echo $CONNECTOR_CONFIG | jq -r .engine_name)"
export FIREBOLT_BUCKET="$(echo $CONNECTOR_CONFIG | jq -r .s3_bucket)"
export AWS_ACCESS_KEY_ID="$(echo $CONNECTOR_CONFIG | jq -r .aws_key_id)"
export AWS_SECRET_ACCESS_KEY="$(echo $CONNECTOR_CONFIG | jq -r .aws_secret_key)"
export AWS_REGION="$(echo $CONNECTOR_CONFIG | jq -r .aws_region)"

export RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"
