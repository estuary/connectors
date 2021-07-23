#!/bin/bash

set -e
export STREAM="estuary-test-$(cat /dev/urandom | tr -dc 'a-z' | head -c 6)"

config_json_template='{
    "awsAccessKeyId": "$AWS_ACCESS_KEY_ID",
    "awsSecretAccessKey": "$AWS_SECRET_ACCESS_KEY",
    "region": "${DEFAULT_AWS_REGION}"
}'

export CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"
root_dir="$(git rev-parse --show-toplevel)"

aws kinesis create-stream --stream-name "$STREAM" --shard-count 3
echo "waiting for stream: $STREAM to exist"
aws kinesis wait stream-exists --stream-name "$STREAM"
echo "created stream: $STREAM"

while IFS="" read -r doc || [ -n "$doc" ]
do
    aws --cli-binary-format base64 kinesis put-record \
        --stream-name "$STREAM" \
        --partition-key "$(head -c 12 /dev/urandom | base64)" \
        --data "$(echo "$doc" | base64)"
done < tests/files/d.jsonl

