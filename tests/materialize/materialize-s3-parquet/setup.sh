#!/bin/bash
set -e

export LOCALSTACK_CONTAINER_NAME=localstack
export LOCALSTACK_ENDPOINT=http://localhost:4566

export TEST_AWS_REGION=us-east-1
export TEST_BUCKET="test-bucket"
export TEST_PATH_PREFIX_SIMPLE="simple"
export TEST_PATH_PREFIX_MULTIPLE_DATATYPES="/multiple-datatypes"

function startLocalStack() {
    docker run -d \
      --rm \
      --name="${LOCALSTACK_CONTAINER_NAME}" \
      --network=host \
      --env "SERVICES=s3" \
      localstack/localstack

    for i in {1..20}; do
        # Wait until the elastic search is ready for serving.
        if curl -o /dev/null -s -I -f "${LOCALSTACK_ENDPOINT}";  then
            echo "localstack started successfully."
            return 0
        fi
        echo "Not ready, retrying ${i}."
        sleep 3
    done
    echo "Localstack logs:"
    docker logs "${LOCALSTACK_CONTAINER_NAME}"
    return 1
}
startLocalStack || bail "failed to start localstack"

aws s3 mb "s3://${TEST_BUCKET}" --endpoint "${LOCALSTACK_ENDPOINT}"

config_json_template='{
    "bucket": "${TEST_BUCKET}",
    "region": "${TEST_AWS_REGION}",
    "endpoint": "${LOCALSTACK_ENDPOINT}",
    "uploadIntervalInSeconds": 2
}'

resources_json_template='[
  {
    "resource": {
      "pathPrefix": "${TEST_BUCKET}/${TEST_PATH_PREFIX_SIMPLE}",
      "compressionType": "snappy"
    },
    "source": "${TEST_COLLECTION_SIMPLE}"
  },
  {
    "resource": {
      "pathPrefix": "${TEST_BUCKET}/${TEST_PATH_PREFIX_MULTIPLE_DATATYPES}",
      "compressionType": "none"
    },
    "source": "${TEST_COLLECTION_MULTIPLE_DATATYPES}"
  }
]'

CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"
export CONNECTOR_CONFIG

RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"
export RESOURCES_CONFIG
