#!/bin/bash

set -e
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:=test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:=test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:=test}"
export KINESIS_ENDPOINT="${KINESIS_ENDPOINT:=http://source-kinesis-db-1.flow-test:4566}"

export TEST_STREAM="estuary-test-$(shuf -zer -n6 {a..z} | tr -d '\0')"
export RESOURCE="{ \"stream\": \"${TEST_STREAM}\", \"syncMode\": \"incremental\" }"

export KINESIS_LOCAL_ENDPOINT="http://localhost:4566"

config_json_template='{
    "awsAccessKeyId": "$AWS_ACCESS_KEY_ID",
    "awsSecretAccessKey": "$AWS_SECRET_ACCESS_KEY",
    "region": "${AWS_DEFAULT_REGION}",
    "advanced": {
        "endpoint": "${KINESIS_ENDPOINT}"
    }
}'

export CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"

docker compose -f source-kinesis/docker-compose.yaml up --detach --wait

aws kinesis create-stream --stream-name "$TEST_STREAM" --shard-count 3 --endpoint-url "${KINESIS_LOCAL_ENDPOINT}"
echo "waiting for stream: $TEST_STREAM to exist"
aws kinesis wait stream-exists --stream-name "$TEST_STREAM" --endpoint-url "${KINESIS_LOCAL_ENDPOINT}"
echo "created stream: $TEST_STREAM"

while IFS="" read -r doc || [ -n "$doc" ]; do
    aws --cli-binary-format base64 kinesis put-record \
        --stream-name "$TEST_STREAM" \
        --partition-key "$(head -c 12 /dev/urandom | base64)" \
        --data "$(echo "$doc" | base64)" \
        --endpoint-url "${KINESIS_LOCAL_ENDPOINT}"
done <tests/files/d.jsonl
