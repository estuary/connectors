#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

export AWS_ACCESS_KEY_ID=flow
export AWS_SECRET_ACCESS_KEY=flow
export AWS_DEFAULT_REGION=us-east-1
export S3_CONTAINER_ENDPOINT_URL=http://storage:9000
export S3_LOCALHOST_ENDPOINT_URL=http://localhost:9000

export TEST_BUCKET="test-bucket"
export TEST_PATH_SIMPLE="${CONNECTOR}/simple"
export TEST_PATH_MULTIPLE_DATATYPES="${CONNECTOR}/multiple-datatypes"

docker compose -f materialize-s3-parquet/docker-compose.yaml up --wait

aws s3 mb "s3://${TEST_BUCKET}" --endpoint-url "${S3_LOCALHOST_ENDPOINT_URL}"

config_json_template='{
    "bucket": "${TEST_BUCKET}",
    "credentials": {
    	"auth_type": "AWSAccessKey",
    	"awsAccessKeyId": "${AWS_ACCESS_KEY_ID}",
    	"awsSecretAccessKey": "${AWS_SECRET_ACCESS_KEY}"
	},
    "region": "${AWS_DEFAULT_REGION}",
    "uploadInterval": "1s",
	"endpoint": "${S3_CONTAINER_ENDPOINT_URL}",
	"use_path_style": true
}'

resources_json_template='[
  {
    "resource": {
      "path": "${TEST_PATH_SIMPLE}"
    },
    "source": "${TEST_COLLECTION_SIMPLE}"
  },
  {
    "resource": {
      "path": "${TEST_PATH_MULTIPLE_DATATYPES}"
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
