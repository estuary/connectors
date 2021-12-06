#!/bin/bash
set -e

# Elastic index names.
export TEST_ES_INDEX_DUPLICATED_KEYS_DELTA="index-duplicated-keys-delta"
export TEST_ES_INDEX_DUPLICATED_KEYS_NON_DELTA="index-duplicated-keys-non-delta"
export TEST_ES_INDEX_MULTIPLE_DATA_TYPES="index-multiple-data-types"

# local elasticsearch docker container name.
export TEST_ES_CONTAINER_NAME=test-es

# Endpoint to access elastic search.
export TEST_ES_ENDPOINT=http://localhost:9200

# start local elasticsearch.
function startElasticsearch() {
    docker run -d --rm --name "${TEST_ES_CONTAINER_NAME}" --network=host --env "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.15.2
    
    for i in {1..20}; do
        # Wait until the elastic search is ready for serving.
        if curl -o /dev/null -s -I -f ${TEST_ES_ENDPOINT}; then
            echo "elastic server started successfully."
            return 0
        fi
        echo "Not ready, retrying ${i}."
        sleep 3
    done
    return 1
}

startElasticsearch || bail "failed to start the elastic search service after 60s."


config_json_template='{
    "endpoint": "${TEST_ES_ENDPOINT}"
}'

resources_json_template='[
  {
    "resource": {
      "index": "${TEST_ES_INDEX_DUPLICATED_KEYS_DELTA}",
      "number_of_shards": 1,
      "delta_updates": true
    },
    "source": "${TEST_COLLECTION_DUPLICATED_KEYS}"
  },
  {
    "resource": {
      "index": "${TEST_ES_INDEX_DUPLICATED_KEYS_NON_DELTA}",
      "number_of_shards": 1,
      "delta_updates": false
    },
    "source": "${TEST_COLLECTION_DUPLICATED_KEYS}"
  },
  {
    "resource": {
      "index": "${TEST_ES_INDEX_MULTIPLE_DATA_TYPES}",
      "number_of_shards": 1,
      "delta_updates":false
    },
    "source": "${TEST_COLLECTION_MULTIPLE_DATATYPES}"
  }
]'

CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"
export CONNECTOR_CONFIG

RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"
export RESOURCES_CONFIG
