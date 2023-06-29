#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

export PINECONE_ENVIRONMENT="${PINECONE_ENVIRONMENT}"
export PINECONE_API_KEY="${PINECONE_API_KEY}"
export OPENAI_API_KEY="${OPENAI_API_KEY}"
export PINECONE_PROJECT_ID="${PINECONE_PROJECT_ID}"

config_json_template='{
   "index":           "$PINECONE_INDEX",
   "environment":     "$PINECONE_ENVIRONMENT",
   "pineconeApiKey":  "$PINECONE_API_KEY",
   "openAiApiKey":    "$OPENAI_API_KEY"
}'

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
    "source": "${TEST_COLLECTION_MULTIPLE_DATATYPES}"
  }
]'

export CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"
export RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"
