#!/bin/bash

set -e
export TEST_STREAM="estuary-test-$(shuf -zer -n6 {a..z} | tr -d '\0')"
export RESOURCE="{ stream: ${TEST_STREAM} }"
# set ID_TYPE to string because parsing CSV files will always result in string values.
export ID_TYPE=string

export GCP_SERVICE_ACCOUNT_KEY_QUOTED=$(echo ${GCP_SERVICE_ACCOUNT_KEY} | jq 'tojson')

config_json_template='{
    "googleCredentials": ${GCP_SERVICE_ACCOUNT_KEY_QUOTED},
    "bucket": "${TEST_STREAM}"
}'

export CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"

gsutil mb -p "$GCP_PROJECT_ID" "gs://${TEST_STREAM}"

root_dir="$(git rev-parse --show-toplevel)"

# We need to exclude the json file from the test because the `id` property there is an integer, and
# this connector expects it to be a string.
for file in $(find ${root_dir}/tests/files -type f -name '*.csv*'); do
    gsutil cp ${file} gs://${TEST_STREAM}/testprefix/$(basename $file)
done

sleep_seconds=30
echo "Sleeping for ${sleep_seconds} seconds to account for filesource clock delta"
sleep $sleep_seconds
