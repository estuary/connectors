#!/bin/bash
set -e

export TEST_STREAM="estuary-test-$(shuf -zer -n6 {a..z} | tr -d '\0')"
export RESOURCE="{ stream: ${TEST_STREAM} }"

# Because Flow uses network=host, the port exposed to Flow is different than the
# one we use when running `docker exec` below.
export CONNECTOR_CONFIG='{
  "bootstrap_servers": ["localhost:9092"]
}'

root_dir="$(git rev-parse --show-toplevel)"
kafkactl_config="$root_dir/tests/source-kafka/kafkactl.yaml"
TOTAL_PARTITIONS=4

if [ -z $(which kafkactl) ]; then
  echo "kafkactl is required"
  exit 1
fi

# Ensure we can connect to a broker.
for i in $(seq 1 10); do
  if [ -n "$(kafkactl --config-file=$kafkactl_config get topics)" ]; then
    break
  else
    if [ $i -ge 10 ]; then
      echo "Can't connect to Kafka. Is the kafkactl config correct?"
      kafkactl --config-file=$kafkactl_config config view
      exit 1
    fi
    sleep 1
  fi
done

# Create the topic with n partitions
kafkactl --config-file=$kafkactl_config create topic $TEST_STREAM --partitions $TOTAL_PARTITIONS

# Seed the topic with documents
for i in $(seq 1 $TOTAL_PARTITIONS); do
  cat $root_dir/tests/files/d.jsonl \
  | jq -cs "map(select(.id % $TOTAL_PARTITIONS == $i - 1)) | .[]" \
  | kafkactl --config-file=$kafkactl_config produce $TEST_STREAM
done
