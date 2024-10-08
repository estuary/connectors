#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

resources_json_template='[
  {
    "resource": {
      "namespace": "simple"
    },
    "source": "${TEST_COLLECTION_SIMPLE}"
  },
  {
    "resource": {
      "namespace": "duplicated-keys"
    },
    "source": "${TEST_COLLECTION_DUPLICATED_KEYS}"
  },
  {
    "resource": {
      "namespace": "multiple-types"
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

export CONNECTOR_CONFIG="$(decrypt_config ${TEST_DIR}/${CONNECTOR}/config.yaml)"
export PINECONE_INDEX="$(echo $CONNECTOR_CONFIG | jq -r .index)"
export PINECONE_API_KEY="$(echo $CONNECTOR_CONFIG | jq -r .pineconeApiKey)"

export RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"
