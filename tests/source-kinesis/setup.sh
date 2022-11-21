#!/bin/bash

set -e
export TEST_STREAM="estuary-test-$(shuf -zer -n6 {a..z} | tr -d '\0')"
export RESOURCE="{ \"stream\": \"${TEST_STREAM}\", \"syncMode\": \"incremental\" }"

config_json_template='{
    "awsAccessKeyId": "$AWS_ACCESS_KEY_ID",
    "awsSecretAccessKey": "$AWS_SECRET_ACCESS_KEY",
    "region": "${DEFAULT_AWS_REGION}"
}'

export CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"
root_dir="$(git rev-parse --show-toplevel)"

aws kinesis create-stream --stream-name "$TEST_STREAM" --shard-count 3
echo "waiting for stream: $TEST_STREAM to exist"
aws kinesis wait stream-exists --stream-name "$TEST_STREAM"
echo "created stream: $TEST_STREAM"

while IFS="" read -r doc || [ -n "$doc" ]
do
    aws --cli-binary-format base64 kinesis put-record \
        --stream-name "$TEST_STREAM" \
        --partition-key "$(head -c 12 /dev/urandom | base64)" \
        --data "$(echo "$doc" | base64)"
done < tests/files/d.jsonl
