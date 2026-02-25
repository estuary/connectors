#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

function query() {
  db=""
  if [ -n "${MYSQL_DATABASE:-}" ]; then
    db="--database=$MYSQL_DATABASE"
  fi


  echo "$1" | docker run -i --rm mysql \
    mysqlsh --sql --json=raw \
    --host=$MYSQL_HOST --port=$MYSQL_PORT \
    --user=$MYSQL_USER --password=$MYSQL_PASSWORD $db \
    | grep -v 'Using a password on the command line interface can be insecure'
}

singlestore_api_token="$(decrypt_config $CONNECTOR_TEST_DIR/api-token.yaml | jq -r '.token')"

workspace_id='e6d3b980-8c6a-4b9f-806e-6502c8b7d30e'
curl -XPOST \
  "https://api.singlestore.com/v1/workspaces/$workspace_id/resume" \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $singlestore_api_token" \
  -d '{"disableAutoSuspend": false}'

max_attempts=180
attempt=0

while [ $attempt -lt $max_attempts ]
do
  state=$(curl -s "https://api.singlestore.com/v1/workspaces/$workspace_id" -H "Authorization: Bearer $singlestore_api_token" | jq -r '.state')
  if [[ "$state" == "ACTIVE" ]]; then
    break
  fi
  
  attempt=$((attempt + 1))
  
  if [ $attempt -eq $max_attempts ]; then
    echo "Error: Workspace did not become ACTIVE after $max_attempts attempts (15 minutes)" >&2
    exit 1
  fi
  
  sleep 5
done

echo "Workspace is now ACTIVE (took $attempt attempts)"

resources_json_template='[
  {
    "resource": {
      "table": "Simple"
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
      "table": "duplicate_keys_delta",
      "delta_updates": true
    },
    "source": "${TEST_COLLECTION_DUPLICATED_KEYS}"
  },
  {
    "resource": {
      "table": "duplicate_keys_delta_exclude_flow_doc",
      "delta_updates": true
    },
    "source": "${TEST_COLLECTION_DUPLICATED_KEYS}",
    "fields": {
      "recommended": true,
      "exclude": [
        "flow_document" 
      ]
    }
  },
  {
    "resource": {
      "table": "Multiple Types"
    },
    "source": "${TEST_COLLECTION_MULTIPLE_DATATYPES}",
    "fields": {
      "recommended": true,
      "exclude": ["nested/id"],
      "include": {
        "nested": {},
        "array_int": {}
      }
    }
  },
  {
    "resource": {
      "table": "Formatted Strings"
    },
    "source": "${TEST_COLLECTION_FORMATTED_STRINGS}",
    "fields": {
      "recommended": true
    }
  },
  {
    "resource": {
      "table": "long-string"
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
  },
  {
    "resource": {
      "table": "timezone_datetimes_delta",
      "delta_updates": true
    },
    "source": "${TEST_COLLECTION_TIMEZONE_DATETIMES}",
    "fields": {
      "recommended": true
    }
  }
]'

export CONNECTOR_CONFIG="$(decrypt_config $CONNECTOR_TEST_DIR/config.yaml)"
export RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"

export MYSQL_HOST="$(echo $CONNECTOR_CONFIG | jq -r .address | cut -d':' -f1)"
export MYSQL_PORT="$(echo $CONNECTOR_CONFIG | jq -r .address | cut -d':' -f2)"
export MYSQL_PASSWORD="$(echo $CONNECTOR_CONFIG | jq -r .password)"
export MYSQL_USER="$(echo $CONNECTOR_CONFIG | jq -r .user)"

query "ATTACH DATABASE $(echo $CONNECTOR_CONFIG | jq -r .database);" || true

export MYSQL_DATABASE="$(echo $CONNECTOR_CONFIG | jq -r .database)"
