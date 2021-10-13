#!/bin/bash
set -e

TEST_BUCKET="estuary-test-$(shuf -zer -n6 {a..z} | tr -d '\0')"
export TEST_BUCKET
export TEST_PATH_PREFIX_SIMPLE="${CONNECTOR}/simple"
export TEST_PATH_PREFIX_MULTIPLE_DATATYPES="${CONNECTOR}/multiple-datatypes"

aws s3api create-bucket --bucket "${TEST_BUCKET}"

config_json_template='{
    "awsAccessKeyId": "${AWS_ACCESS_KEY_ID}",
    "awsSecretAccessKey": "${AWS_SECRET_ACCESS_KEY}",
    "bucket": "${TEST_BUCKET}",
    "region": "${DEFAULT_AWS_REGION}",
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
    "source": "${TEST_COLLECTION_MULTIPLE_DATATYPES}"
  }
]'

CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"
export CONNECTOR_CONFIG

RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"
export RESOURCES_CONFIG
