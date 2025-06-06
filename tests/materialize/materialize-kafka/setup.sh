#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset


config_json_template='{
    "bootstrap_servers": "materialize-kafka-db-1.flow-test:9092",
    "topic_partitions": 3,
    "topic_replication_factor": 1,
    "message_format": "JSON",
    "compression": "lz4"
}'

resources_json_template='[
  {
    "resource": {
      "topic": "simple"
    },
    "source": "${TEST_COLLECTION_SIMPLE}"
  }
]'

export CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"
export RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"

LISTENER_HOST="materialize-kafka-db-1.flow-test"  docker compose -f materialize-kafka/docker-compose.yaml up --detach --wait
