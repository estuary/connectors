#!/bin/bash

set -e
export TEST_BUCKET="estuary-test-$(shuf -zer -n6 {a..z} | tr -d '\0')"
export RESOURCE="{ stream: ${TEST_BUCKET} }"

export GCP_SERVICE_ACCOUNT_KEY_QUOTED=$(echo ${GCP_SERVICE_ACCOUNT_KEY} | jq 'tojson')

config_json_template='{
    "googleCredentials": ${GCP_SERVICE_ACCOUNT_KEY_QUOTED},
    "bucket": "${TEST_BUCKET}"
}'

export CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"

gsutil mb -p "$GCP_PROJECT_ID" "gs://${TEST_BUCKET}"

root_dir="$(git rev-parse --show-toplevel)"

for file in $(find ${root_dir}/tests/files -type f); do
    gsutil cp ${file} gs://${TEST_BUCKET}/testprefix/$(basename $file)
done

sleep_seconds=30
echo "Sleeping for ${sleep_seconds} seconds to account for filesource clock delta"
sleep $sleep_seconds
