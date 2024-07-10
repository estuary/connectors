#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

resources_json_template='[
  {
    "resource": {
      "table": "simple_delta",
      "delta_updates": true
    },
    "source": "${TEST_COLLECTION_SIMPLE}"
  }
]'

function decrypt_config {
  sops --output-type json --decrypt $1 | jq 'walk( if type == "object" then with_entries(.key |= rtrimstr("_sops")) else . end)' 
}

export CONNECTOR_CONFIG="$(decrypt_config ${TEST_DIR}/${CONNECTOR}/config.yaml)"
export AWS_ACCESS_KEY_ID="$(echo $CONNECTOR_CONFIG | jq -r .aws_access_key_id)"
export AWS_SECRET_ACCESS_KEY="$(echo $CONNECTOR_CONFIG | jq -r .aws_secret_access_key)"
export AWS_REGION="$(echo $CONNECTOR_CONFIG | jq -r .region)"
export AWS_BUCKET="$(echo $CONNECTOR_CONFIG | jq -r .bucket)"
export PREFIX="$(echo $CONNECTOR_CONFIG | jq -r .prefix)"
export NAMESPACE=$(echo $CONNECTOR_CONFIG | jq -r .namespace)

export RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"

export S3_DATA_URI="s3://${AWS_BUCKET}/${PREFIX}"

echo "Creating database: ${NAMESPACE}"
aws glue create-database --database-input "{\"Name\": \"${NAMESPACE}\"}"
