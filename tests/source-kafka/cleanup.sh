#!/bin/bash
set -e

root_dir="$(git rev-parse --show-toplevel)"
kafkactl_config="$root_dir/tests/source-kafka/kafkactl.yaml"

# Remove the test topic
kafkactl --config-file=$kafkactl_config delete topic $TEST_STREAM
