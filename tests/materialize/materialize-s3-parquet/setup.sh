#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

export LOCALSTACK_CONTAINER_NAME=localstack
export LOCALSTACK_S3_ENDPOINT=http://${LOCALSTACK_CONTAINER_NAME}.flow-test:4566
export LOCALSTACK_S3_LOCAL_ENDPOINT=http://localhost:4566

# Dummy configs for awscli to access localstack.
export AWS_ACCESS_KEY_ID=test_key
export AWS_SECRET_ACCESS_KEY=test_secret
export AWS_DEFAULT_REGION=us-east-1

export TEST_BUCKET="test-bucket"
export TEST_PATH_PREFIX_SIMPLE="${CONNECTOR}/simple"
export TEST_PATH_PREFIX_MULTIPLE_DATATYPES="${CONNECTOR}/multiple-datatypes"

function startLocalStack() {
    docker run -d \
      --rm \
      --user 0 \
      --network "flow-test" \
      --publish 4566:4566 \
      --name="${LOCALSTACK_CONTAINER_NAME}" \
      --env "SERVICES=s3" \
      localstack/localstack

    for i in {1..20}; do
        # Wait until the local stack is ready for serving.
        if aws s3 ls --endpoint-url "${LOCALSTACK_S3_LOCAL_ENDPOINT}";  then
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
startLocalStack || bail "failed to start localstack."

aws s3 mb "s3://${TEST_BUCKET}" --endpoint "${LOCALSTACK_S3_LOCAL_ENDPOINT}"

config_json_template='{
    "bucket": "${TEST_BUCKET}",
    "region": "${AWS_DEFAULT_REGION}",
    "advanced": {
      "endpoint": "${LOCALSTACK_S3_ENDPOINT}"
    },
    "uploadIntervalInSeconds": 2
}'

resources_json_template='[
  {
    "resource": {
      "pathPrefix": "${TEST_PATH_PREFIX_SIMPLE}",
      "compressionType": "snappy"
    },
    "source": "${TEST_COLLECTION_SIMPLE}"
  },
  {
    "resource": {
      "pathPrefix": "${TEST_PATH_PREFIX_MULTIPLE_DATATYPES}",
      "compressionType": "none"
    },
    "source": "${TEST_COLLECTION_MULTIPLE_DATATYPES}",
    "fields": {
      "recommended": true,
      "include": {
        "nested": {},
        "array_int": {}
      }
    }
  }
]'

export CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"
export RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"
