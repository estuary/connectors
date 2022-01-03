#!/bin/bash
set -e

export TEST_BUCKET="estuary-connector-test"

TEST_ID="${CONNECTOR}/$(date '+%Y-%M-%dT%H-%M-%S')_$(shuf -zer -n6 {a..z} | tr -d '\0')"
export TEST_ID="${TEST_ID}"
export TEST_PATH_PREFIX_SIMPLE="simple"
export TEST_PATH_PREFIX_MULTIPLE_DATATYPES="multiple-datatypes"

config_json_template='{
    "bucket": "${TEST_BUCKET}",
    "awsAccessKeyId": "${AWS_ACCESS_KEY_ID}",
    "awsSecretAccessKey": "${AWS_SECRET_ACCESS_KEY}",
    "region": "${AWS_DEFAULT_REGION}",
    "uploadIntervalInSeconds": 2
}'

resources_json_template='[
  {
    "resource": {
      "pathPrefix": "${TEST_ID}/${TEST_PATH_PREFIX_SIMPLE}",
      "compressionType": "snappy"
    },
    "source": "${TEST_COLLECTION_SIMPLE}"
  },
  {
    "resource": {
      "pathPrefix": "${TEST_ID}/${TEST_PATH_PREFIX_MULTIPLE_DATATYPES}",
      "compressionType": "none"
    },
    "source": "${TEST_COLLECTION_MULTIPLE_DATATYPES}"
  }
]'

CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"
export CONNECTOR_CONFIG

RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"
export RESOURCES_CONFIG
