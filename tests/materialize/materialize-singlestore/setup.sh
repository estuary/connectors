#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

function query() {
  echo "$1" | docker run -e MYSQL_HOST -e MYSQL_PORT -e MYSQL_DATABASE -e MYSQL_PASSWORD -e MYSQL_USER -i --rm mysql \
    mysqlsh --sql --json=raw \
    --host=$MYSQL_HOST --port=$MYSQL_PORT \
    --user=$MYSQL_USER --password=$MYSQL_PASSWORD \
    --database $MYSQL_DATABASE
}

singlestore_api_token="$(decrypt_config ${TEST_DIR}/${CONNECTOR}/api-token.yaml | jq -r '.token')"

workspace_id='e3db037e-1201-4e0b-9622-28a3deccafd6'
curl -XPOST \
  "https://api.singlestore.com/v1/workspaces/$workspace_id/resume" \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $singlestore_api_token" \
  -d '{"disableAutoSuspend": false}'

while :
do
  state=$(curl -s "https://api.singlestore.com/v1/workspaces/$workspace_id" -H "Authorization: Bearer $singlestore_api_token" | jq -r '.state')
  if [[ "$state" == "ACTIVE" ]]; then
    break
  fi
  sleep 5
done

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
  }
]'

export CONNECTOR_CONFIG="$(decrypt_config ${TEST_DIR}/${CONNECTOR}/config.yaml)"
export RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"

export MYSQL_HOST="$(echo $CONNECTOR_CONFIG | jq -r .address | cut -d':' -f1)"
export MYSQL_PORT="$(echo $CONNECTOR_CONFIG | jq -r .address | cut -d':' -f2)"
export MYSQL_DATABASE="$(echo $CONNECTOR_CONFIG | jq -r .database)"
export MYSQL_PASSWORD="$(echo $CONNECTOR_CONFIG | jq -r .password)"
export MYSQL_USER="$(echo $CONNECTOR_CONFIG | jq -r .user)"
