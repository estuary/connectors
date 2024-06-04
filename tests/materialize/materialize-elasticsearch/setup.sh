#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

# Endpoint to access elastic search.
export TEST_ES_ENDPOINT=http://materialize-elasticsearch-db-1.flow-test:9200
export TEST_ES_LOCAL_ENDPOINT=http://localhost:9200
export TEST_ES_USERNAME=elastic
export TEST_ES_PASSWORD=elastic

docker compose -f materialize-elasticsearch/docker-compose.yaml up --wait

config_json_template='{
    "endpoint": "${TEST_ES_ENDPOINT}",
    "hardDelete": true,
    "credentials": {
      "username": "${TEST_ES_USERNAME}",
      "password": "${TEST_ES_PASSWORD}"
    },
    "advanced": {
      "number_of_replicas": 0
    }
}'

resources_json_template='[
  {
    "resource": {
      "index": "index-simple",
      "number_of_shards": 1
    },
    "source": "${TEST_COLLECTION_SIMPLE}"
  },
  {
    "resource": {
      "index": "index-duplicated-keys-standard",
      "number_of_shards": 1,
      "delta_updates": false
    },
    "source": "${TEST_COLLECTION_DUPLICATED_KEYS}"
  },
  {
    "resource": {
      "index": "index-duplicated-keys-delta",
      "number_of_shards": 1,
      "delta_updates": true
    },
    "source": "${TEST_COLLECTION_DUPLICATED_KEYS}"
  },
  {
    "resource": {
      "index": "index-duplicated-keys-delta-exclude-flow-doc",
      "number_of_shards": 1,
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
      "index": "index-multiple-data-types",
      "number_of_shards": 1,
      "delta_updates":false
    },
    "source": "${TEST_COLLECTION_MULTIPLE_DATATYPES}",
    "fields": {
      "recommended": true,
      "exclude": ["nested/id"],
      "include": {
        "nested": {}
      }
    }
  },
  {
    "resource": {
      "index": "index-formatted-strings",
      "number_of_shards": 1
    },
    "source": "${TEST_COLLECTION_FORMATTED_STRINGS}"
  },
  {
    "resource": {
      "index": "index-deletions",
      "number_of_shards": 1
    },
    "source": "${TEST_COLLECTION_DELETIONS}"
  }
]'

CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"
export CONNECTOR_CONFIG

RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"
export RESOURCES_CONFIG
