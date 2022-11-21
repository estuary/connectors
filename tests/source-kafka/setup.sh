#!/bin/bash
set -e

export TEST_STREAM="estuary-test-$(shuf -zer -n6 {a..z} | tr -d '\0')"
export RESOURCE="{ \"stream\": \"${TEST_STREAM}\", \"syncMode\": \"incremental\" }"

# Because Flow uses network=host, the port exposed to Flow is different than the
# one we use when running `docker exec` below.
export CONNECTOR_CONFIG='{
  "bootstrap_servers": ["infra-kafka-1.flow-test:9092"],
  "authentication": {
    "mechanism": "SCRAM-SHA-256",
    "username": "alice",
    "password": "alice-pass"
  },
  "tls": null
}'

root_dir="$(git rev-parse --show-toplevel)"
kafkactl_config="$root_dir/tests/source-kafka/kafkactl.yaml"
TOTAL_PARTITIONS=4

function kctl() {
  docker run -i --network flow-test --mount "type=bind,src=$kafkactl_config,target=/kafkactl.yaml" deviceinsight/kafkactl --config-file=/kafkactl.yaml $@
}

if [ -z $(which kafkactl) ]; then
  echo "kafkactl is required"
  exit 1
fi

# Ensure we can connect to a broker.
for i in $(seq 1 10); do
  if [ -n "$(kctl get topics)" ]; then
    break
  else
    if [ $i -ge 10 ]; then
      echo "Can't connect to Kafka. Is the kafkactl config correct?"
      kctl config view
      exit 1
    fi
    sleep 2
  fi
done

# Create the topic with n partitions
kctl create topic $TEST_STREAM --partitions $TOTAL_PARTITIONS

# Seed the topic with documents
for i in $(seq 1 $TOTAL_PARTITIONS); do
  cat $root_dir/tests/files/d.jsonl \
  | jq -cs "map(select(.id % $TOTAL_PARTITIONS == $i - 1)) | .[]" \
  | kctl produce $TEST_STREAM
done
