#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

docker compose -f materialize-mongodb/docker-compose.yaml up -d --wait

config_json_template='{
   "address":  "mongodb://materialize-mongodb-mongo-1.flow-test:27017?authSource=admin",
   "database": "test",
   "password": "flow",
   "user":     "flow"
}'

resources_json_template='[
  {
    "resource": {
      "collection": "duplicate_keys_standard"
    },
    "source": "${TEST_COLLECTION_DUPLICATED_KEYS}"
  },
  {
    "resource": {
      "collection": "duplicate_keys_delta",
      "delta_updates": true
    },
    "source": "${TEST_COLLECTION_DUPLICATED_KEYS}"
  },
  {
    "resource": {
      "collection": "Simple"
    },
    "source": "${TEST_COLLECTION_SIMPLE}"
  }
]'

export CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"
export RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"
