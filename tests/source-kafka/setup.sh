#!/bin/bash
set -e

export TEST_STREAM="estuary-test-$(shuf -zer -n6 {a..z} | tr -d '\0')"
export RESOURCE="{\"topic\": \"${TEST_STREAM}\"}"
export CONNECTOR_CONFIG='{"bootstrap_servers": "source-kafka-db-1.flow-test:9092"}'

LISTENER_HOST="source-kafka-db-1.flow-test" docker compose -f source-kafka/docker-compose.yaml up --wait --detach

docker exec source-kafka-db-1 sh -c "/bin/kafka-topics --create --topic ${TEST_STREAM} --bootstrap-server localhost:9092"
docker cp tests/files/d.jsonl source-kafka-db-1:/
docker exec source-kafka-db-1 sh -c "cat /d.jsonl | /bin/kafka-console-producer --topic ${TEST_STREAM} --bootstrap-server localhost:9092"
