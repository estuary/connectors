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

function decrypt_config {
  sops --output-type json --decrypt $1 | jq 'walk( if type == "object" then with_entries(.key |= rtrimstr("_sops")) else . end)' 
}

export CONNECTOR_CONFIG="$(decrypt_config $CONNECTOR_TEST_DIR/config.yaml)"
export AWS_ACCESS_KEY_ID="$(echo $CONNECTOR_CONFIG | jq -r .aws_access_key_id)"
export AWS_SECRET_ACCESS_KEY="$(echo $CONNECTOR_CONFIG | jq -r .aws_secret_access_key)"
export AWS_REGION="$(echo $CONNECTOR_CONFIG | jq -r .region)"
export AWS_BUCKET="$(echo $CONNECTOR_CONFIG | jq -r .bucket)"
export PREFIX="$(echo $CONNECTOR_CONFIG | jq -r .prefix)"
export NAMESPACE=$(echo $CONNECTOR_CONFIG | jq -r .namespace)
export WAREHOUSE=$(echo $CONNECTOR_CONFIG | jq -r .catalog.warehouse)

export RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"

export S3_DATA_URI="s3://${AWS_BUCKET}/${PREFIX}"

# Start the rest catalog.
docker compose -f materialize-s3-iceberg/docker-compose.yaml up --wait

# Create the test warehouse.
create_warehouse_json_template='{
  "warehouse-name": "${WAREHOUSE}",
  "project-id": "00000000-0000-0000-0000-000000000000",
  "storage-profile": {
    "type": "s3",
    "bucket": "${AWS_BUCKET}",
    "region": "${AWS_REGION}",
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
