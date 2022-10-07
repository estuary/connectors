#!/bin/bash
set -e

root_dir="$(git rev-parse --show-toplevel)"
kafkactl_config="$root_dir/tests/source-kafka/kafkactl.yaml"

function kctl() {
  docker run -i --network flow-test --mount "type=bind,src=$kafkactl_config,target=/kafkactl.yaml" deviceinsight/kafkactl --config-file=/kafkactl.yaml $@
}

# Remove the test topic
kctl delete topic $TEST_STREAM
