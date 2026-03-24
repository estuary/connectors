#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

# TODO(whb): The "formatted strings" collection is not included becuase the
# extreme minimum & maximum timestamps crash something within pyiceberg. I don't
# want to make code changes for the connector to handle this via clamping etc.
# yet because these values are perfectly valid to write to a parquet file, and
# the limitation seems to be only in the pyiceberg implementation. I am hoping
# the underlying siutation improves, or we start to write Iceberg data files
# differently, before this becomes a more pressing problem.
resources_json_template='[
  {
    "resource": {
      "table": "simple_delta",
      "delta_updates": true
    },
    "source": "${TEST_COLLECTION_SIMPLE}"
  },
  {
    "resource": {
      "table": "duplicate_keys_delta",
      "delta_updates": true
    },
    "source": "${TEST_COLLECTION_DUPLICATED_KEYS}"
  },
  {
    "resource": {
      "table": "multiple_types_delta",
      "delta_updates": true
    },
    "source": "${TEST_COLLECTION_MULTIPLE_DATATYPES}",
    "fields": {
      "recommended": true,
      "exclude": ["nested/id"],
      "include": {
        "nested": {},
        "array_int": {},
        "multiple": {}
      }
    }
  }
]'

export RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"

export AWS_BUCKET=test-bucket
export AWS_ACCESS_KEY_ID=flow
export AWS_SECRET_ACCESS_KEY=flow
export WAREHOUSE=test_warehouse
export NAMESPACE=iceberg-test
export AWS_REGION=us-east-1
export PREFIX=test-data
export S3_ENDPOINT=http://storage:9001
export S3_PATH_STYLE_ACCESS=true

config_json_template='{
  "bucket": "${AWS_BUCKET}",
  "aws_access_key_id": "${AWS_ACCESS_KEY_ID}",
  "aws_secret_access_key": "${AWS_SECRET_ACCESS_KEY}",
  "namespace": "${NAMESPACE}",
  "region": "${AWS_REGION}",
  "upload_interval": "PT5M",
  "prefix": "${PREFIX}",
  "s3_endpoint": "${S3_ENDPOINT}",
  "catalog": {
    "catalog_type": "Iceberg REST Server",
    "uri": "http://server:8080/catalog",
    "token": "TODO",
    "warehouse": "${WAREHOUSE}"
  }
}'
export CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"
echo $CONNECTOR_CONFIG


# Start the rest catalog.
docker compose -f materialize-s3-iceberg/docker-compose.yaml up --wait
# Give extra time to be ready for external connections.
sleep 5

# Create the bucket
curl -s -X PUT http://localhost:9000/${AWS_BUCKET} --aws-sigv4 "aws:amz:us-east-1:s3" --user ${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}

# Create the test warehouse.
create_warehouse_json_template='{
  "warehouse-name": "${WAREHOUSE}",
  "project-id": "00000000-0000-0000-0000-000000000000",
  "storage-profile": {
    "type": "s3",
    "bucket": "${AWS_BUCKET}",
    "region": "${AWS_REGION}",
    "endpoint": "${S3_ENDPOINT}",
    "path-style-access": ${S3_PATH_STYLE_ACCESS},
    "sts-enabled": false
  },
  "storage-credential": {
    "type": "s3",
    "credential-type": "access-key",
    "aws-access-key-id": "${AWS_ACCESS_KEY_ID}",
    "aws-secret-access-key": "${AWS_SECRET_ACCESS_KEY}"
  }
}'

curl -X POST -H "Content-Type: application/json" -d "$(echo "$create_warehouse_json_template" | envsubst | jq -c)" http://localhost:8090/management/v1/warehouse
