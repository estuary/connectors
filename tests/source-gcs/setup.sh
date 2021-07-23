#!/bin/bash

set -e
export STREAM="estuary-test-$(cat /dev/urandom | tr -dc 'a-z' | head -c 6)"

config_json_template='{
    "googleCredentials": $GCP_SERVICE_ACCOUNT_KEY,
    "bucket": "${STREAM}"
}'

export CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"

gsutil mb -p "$GCP_PROJECT_ID" "gs://${STREAM}"

root_dir="$(git rev-parse --show-toplevel)"

for file in $(find ${root_dir}/tests/files -type f); do
    gsutil cp ${file} gs://${STREAM}/testprefix/$(basename $file)
done

sleep_seconds=30
echo "Sleeping for ${sleep_seconds} seconds to account for filesource clock delta"
sleep $sleep_seconds
