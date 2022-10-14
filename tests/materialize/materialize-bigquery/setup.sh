#!/bin/bash

set -e

# Ensure canonical Postgres environment variables are set.
export PROJECT_ID="${GCP_BQ_PROJECT_ID}"
export DATASET="${GCP_BQ_DATASET}"
export REGION="${GCP_BQ_REGION}"
export BUCKET="${GCP_BQ_BUCKET}"
export BASE_64_CREDENTIALS_JSON=$(echo ${GCP_SERVICE_ACCOUNT_KEY} | base64)

config_json_template='{
   "project_id":        "$PROJECT_ID",
   "dataset":           "$DATASET",
   "region":            "$REGION",
   "bucket":            "$BUCKET",
   "credentials_json":  "$BASE_64_CREDENTIALS_JSON"
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
      "table": "duplicate_keys"
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
        "array_int": {}
      }
    }
  }
]'

export CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"
echo "Connector configuration is: ${CONNECTOR_CONFIG}".

export RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"
echo "Resources configuration is: ${RESOURCES_CONFIG}".
