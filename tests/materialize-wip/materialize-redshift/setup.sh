#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

export REDSHIFT_ADDRESS="${REDSHIFT_ADDRESS}"
export REDSHIFT_USER="${REDSHIFT_USER}"
export REDSHIFT_PASSWORD="${REDSHIFT_PASSWORD}"
export REDSHIFT_DATABASE="${REDSHIFT_DATABASE}"
export REDSHIFT_BUCKET="${REDSHIFT_BUCKET}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}"
export AWS_REGION="${AWS_REGION}"

config_json_template='{
   "address":             "$REDSHIFT_ADDRESS",
   "database":            "$REDSHIFT_DATABASE",
   "password":            "$REDSHIFT_PASSWORD",
   "user":                "$REDSHIFT_USER",
   "bucket":              "$REDSHIFT_BUCKET",
   "awsAccessKeyId":      "$AWS_ACCESS_KEY_ID",
   "awsSecretAccessKey":  "$AWS_SECRET_ACCESS_KEY",
   "region":              "$AWS_REGION"
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
        "array_int": {}
      }
    }
  },
  {
    "resource": {
      "table": "formatted_strings"
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
