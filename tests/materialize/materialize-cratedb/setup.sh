#!/bin/bash
set -o errexit
set -o pipefail
set -o nounset

# Ensure canonical CrateDB environment variables are set.
# It's ok to set 'crate' as password, even though the default user 'crate' doesn't have a password.
export CRHOST="${CRHOST:=materialize-cratedb-cratedb-1.flow-test}"
export CRPORT="${CRPORT:=5432}"
export CRDATABASE="${CRDATABASE:=crate}"
export CRPASSWORD="${CRPASSWORD:=crate}"
export CRUSER="${CRUSER:=crate}"
docker compose -f materialize-cratedb/docker-compose.yaml up --detach

# Give it time to start.
sleep 5
config_json_template='{
   "address":  "$CRHOST:$CRPORT",
   "database": "$CRDATABASE",
   "password": "$CRPASSWORD",
   "user":     "$CRUSER",
   "schema":   "doc",
   "hardDelete": true
}'
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
  "resource":{
      "table": "underscore_column"
    },
    "source": "${TEST_COLLECTION_UNDERSCORE_COLUMN}"
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
      "table": "MultipleTypes"
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
      "table": "FormattedStrings"
    },
    "source": "${TEST_COLLECTION_FORMATTED_STRINGS}",
    "fields": {
      "recommended": true
    }
  },
  {
    "resource": {
      "table": "deletions"
    },
    "source": "${TEST_COLLECTION_DELETIONS}"
  }
]'
export CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"
export RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"