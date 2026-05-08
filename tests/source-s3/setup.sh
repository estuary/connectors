#!/bin/bash

set -e

export TEST_STREAM="estuary-test-$(shuf -zer -n6 {a..z} | tr -d '\0')"
export RESOURCE="{ \"stream\": \"${TEST_STREAM}\", \"syncMode\": \"incremental\", \"namespace\": \"backward-compatibility-test\", \"cursorField\": [] }"
# set ID_TYPE to string because parsing CSV files will always result in string values.
export ID_TYPE=string

export RUSTFS_S3_ENDPOINT=http://storage.flow-test:4566
export RUSTFS_S3_LOCAL_ENDPOINT=http://localhost:4566

# Dummy configs for awscli to access rustfs.
export AWS_PAGER=""
export AWS_ACCESS_KEY_ID=test_key
export AWS_SECRET_ACCESS_KEY=test_secret
export AWS_DEFAULT_REGION=us-east-1

config_json_template='{
    "awsAccessKeyId": "${AWS_ACCESS_KEY_ID}",
    "awsSecretAccessKey": "${AWS_SECRET_ACCESS_KEY}",
    "bucket": "${TEST_STREAM}",
    "region": "${AWS_DEFAULT_REGION}",
    "advanced": {
      "endpoint": "${RUSTFS_S3_ENDPOINT}"
    }
}'

export CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"

docker compose -f source-s3/docker-compose.yaml up --wait

echo "RustFS logs:"
docker compose logs storage

aws s3 mb "s3://${TEST_STREAM}" --endpoint "${RUSTFS_S3_LOCAL_ENDPOINT}"

root_dir="$(git rev-parse --show-toplevel)"

# We need to exclude the json file from the test because the `id` property there is an integer, and
# this connector expects it to be a string.
for file in $(find ${root_dir}/tests/files -type f -name '*.csv*'); do
    aws s3 cp ${file} s3://${TEST_STREAM}/testprefix/$(basename $file) --endpoint "${RUSTFS_S3_LOCAL_ENDPOINT}"
done

# add an empty prefix to ensure that it gets filtered out
aws s3api put-object --bucket "$TEST_STREAM" --key "testprefix/" --endpoint "${RUSTFS_S3_LOCAL_ENDPOINT}"

sleep_seconds=30
echo "Sleeping for ${sleep_seconds} seconds to account for filesource clock delta"
sleep $sleep_seconds
