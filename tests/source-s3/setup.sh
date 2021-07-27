#!/bin/bash

set -e

export TEST_BUCKET="estuary-test-$(shuf -zer -n6 {a..z} | tr -d '\0')"
export RESOURCE="{ stream: ${TEST_BUCKET} }"

config_json_template='{
    "awsAccessKeyId": "$AWS_ACCESS_KEY_ID",
    "awsSecretAccessKey": "$AWS_SECRET_ACCESS_KEY",
    "bucket": "${TEST_BUCKET}",
    "region": "${DEFAULT_AWS_REGION}"
}'

export CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"

aws s3api create-bucket --bucket $TEST_BUCKET --create-bucket-configuration LocationConstraint="${DEFAULT_AWS_REGION}"

root_dir="$(git rev-parse --show-toplevel)"

for file in $(find ${root_dir}/tests/files -type f); do
    aws s3 cp ${file} s3://${TEST_BUCKET}/testprefix/$(basename $file)
done

sleep_seconds=30
echo "Sleeping for ${sleep_seconds} seconds to account for filesource clock delta"
sleep $sleep_seconds
