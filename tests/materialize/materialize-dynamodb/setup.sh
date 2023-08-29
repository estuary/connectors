#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:=test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:=test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:=test}"
export DYNAMODB_ENDPOINT="${DYNAMODB_ENDPOINT:=http://materialize-dynamodb-db-1.flow-test:8000}"
export DYNAMODB_LOCAL_ENDPOINT="http://localhost:8000"

config_json_template='{
    "awsAccessKeyId": "${AWS_ACCESS_KEY_ID}",
    "awsSecretAccessKey": "${AWS_SECRET_ACCESS_KEY}",
    "region": "${AWS_DEFAULT_REGION}",
    "advanced": {
        "endpoint": "${DYNAMODB_ENDPOINT}"
    }
}'

resources_json_template='[
  {
    "resource": {
      "table": "simple"
    },
    "source": "${TEST_COLLECTION_SIMPLE}",
    "fields": {
      "recommended": true,
      "include": {
        "canary": {}
      }
    }
  },
  {
    "resource": {
      "table": "duplicated-keys-standard"
    },
    "source": "${TEST_COLLECTION_DUPLICATED_KEYS}",
    "fields": {
      "recommended": true,
      "include": {
        "int": {},
        "str": {}
      }
    }
  },
  {
    "resource": {
      "table": "duplicated-keys-delta",
      "delta_updates": true
    },
    "source": "${TEST_COLLECTION_DUPLICATED_KEYS}",
    "fields": {
      "recommended": true,
      "include": {
        "int": {},
        "str": {}
      }
    }
  },
  {
    "resource": {
      "table": "duplicated-keys-delta-exclude-flow-doc",
      "delta_updates": true
    },
    "source": "${TEST_COLLECTION_DUPLICATED_KEYS}",
    "fields": {
      "recommended": true,
      "exclude": [
        "flow_document" 
      ],
      "include": {
        "int": {},
        "str": {}
      }
    }
  },
  {
    "resource": {
      "table": "multiple-types"
    },
    "source": "${TEST_COLLECTION_MULTIPLE_DATATYPES}",
    "fields": {
      "recommended": true,
      "include": {
        "str_field": {},
        "float_field": {},
        "bool_field": {},
        "nullable_int": {},
        "array_int": {},
        "nested": {},
        "multiple": {}
      }
    }
  },
  {
    "resource": {
      "table": "formatted-strings"
    },
    "source": "${TEST_COLLECTION_FORMATTED_STRINGS}",
    "fields": {
      "recommended": true,
      "include": {
        "int_and_str": {},
        "num_and_str": {},
        "int_str": {},
        "num_str": {},
        "datetime": {},
        "date": {},
        "time": {}
      }
    }
  }
]'

export CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"
export RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"

docker compose -f materialize-dynamodb/docker-compose.yaml up --detach

retry_counter=0
while true; do
  if aws dynamodb list-tables --endpoint-url "${DYNAMODB_LOCAL_ENDPOINT}"; then
    echo "DynamoDB container ready"
    break
  fi

  retry_counter=$((retry_counter + 1))
  if [[ "$retry_counter" -eq "30" ]]; then
    bail "Timeout reached"
  fi

  echo "DynamoDB container not ready, retrying ${retry_counter}"
  sleep 1
done
