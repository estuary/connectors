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
  }
]'

export CONNECTOR_CONFIG="$(decrypt_config ${TEST_DIR}/${CONNECTOR}/config.yaml)"
export GCP_BQ_PROJECT_ID="$(echo $CONNECTOR_CONFIG | jq -r .project_id)"
export GCP_BQ_DATASET="$(echo $CONNECTOR_CONFIG | jq -r .dataset)"
export GCP_BQ_REGION="$(echo $CONNECTOR_CONFIG | jq -r .region)"
export GCP_BQ_BUCKET="$(echo $CONNECTOR_CONFIG | jq -r .bucket)"
export GCP_SERVICE_ACCOUNT_KEY=$(echo $CONNECTOR_CONFIG | jq -r .credentials_json)

export RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"
