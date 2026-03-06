#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

export CLICKHOUSE_HOST="${CLICKHOUSE_HOST:=materialize-clickhouse-clickhouse-1.flow-test}"
export CLICKHOUSE_PORT="${CLICKHOUSE_PORT:=9000}"
export CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:=flow}"
export CLICKHOUSE_USER="${CLICKHOUSE_USER:=flow}"
export CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:=flow}"

docker compose -f materialize-clickhouse/docker-compose.yaml up --wait
# Give ClickHouse extra time to be ready for external connections.
sleep 5

function query() {
  docker exec materialize-clickhouse-clickhouse-1 clickhouse-client \
    --host=127.0.0.1 --port=$CLICKHOUSE_PORT --user=$CLICKHOUSE_USER --password=$CLICKHOUSE_PASSWORD \
    --database=$CLICKHOUSE_DATABASE --query="$1"
}

config_json_template='{
   "address":  "$CLICKHOUSE_HOST:$CLICKHOUSE_PORT",
   "credentials": {
     "auth_type": "user_password",
     "username": "$CLICKHOUSE_USER",
     "password": "$CLICKHOUSE_PASSWORD"
   },
   "database": "$CLICKHOUSE_DATABASE",
   "hardDelete": true
}'

resources_json_template='[
  {
    "resource": {
      "table": "simple"
    },
    "source": "${TEST_COLLECTION_SIMPLE}"
  },
  {
    "resource": {
      "table": "duplicate_keys_standard"
    },
    "source": "${TEST_COLLECTION_DUPLICATED_KEYS}"
  },
  {
    "resource": {
      "table": "multiple_types"
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
  },
  {
    "resource": {
      "table": "formatted_strings"
    },
    "source": "${TEST_COLLECTION_FORMATTED_STRINGS}",
    "fields": {
      "recommended": true
    }
  },
  {
    "resource": {
      "table": "long_string"
    },
    "source": "${TEST_COLLECTION_LONG_STRING}"
  },
  {
    "resource": {
      "table": "unsigned_bigint"
    },
    "source": "${TEST_COLLECTION_UNSIGNED_BIGINT}"
  },
  {
    "resource": {
      "table": "deletions"
    },
    "source": "${TEST_COLLECTION_DELETIONS}"
  },
  {
    "resource": {
      "table": "all_key_types_part_one"
    },
    "source": "${TEST_COLLECTION_ALL_KEY_TYPES_PART_ONE}"
  },
  {
    "resource": {
      "table": "all_key_types_part_two"
    },
    "source": "${TEST_COLLECTION_ALL_KEY_TYPES_PART_TWO}"
  },
  {
    "resource": {
      "table": "all_key_types_part_three"
    },
    "source": "${TEST_COLLECTION_ALL_KEY_TYPES_PART_THREE}"
  },
  {
    "resource": {
      "table": "fields_with_projections"
    },
    "source": "${TEST_COLLECTION_FIELDS_WITH_PROJECTIONS}",
    "fields": {
      "recommended": true,
      "exclude": ["original_field"],
      "include": {
        "another_field": {},
        "projected_another": {}
      }
    }
  },
  {
    "resource": {
      "table": "many_columns"
    },
    "source": "${TEST_COLLECTION_MANY_COLUMNS}",
    "fields": {
      "recommended": true
    }
  },
  {
    "resource": {
      "table": "timezone_datetimes_standard"
    },
    "source": "${TEST_COLLECTION_TIMEZONE_DATETIMES}",
    "fields": {
      "recommended": true
    }
  }
]'

export CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"
export RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"
