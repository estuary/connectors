#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

export DATABRICKS_HOST_NAME="${DATABRICKS_HOST_NAME}"
export DATABRICKS_HTTP_PATH="${DATABRICKS_HTTP_PATH}"
export DATABRICKS_CATALOG="${DATABRICKS_CATALOG}"
export DATABRICKS_ACCESS_TOKEN="${DATABRICKS_ACCESS_TOKEN}"

export DBSQLCLI_HOST_NAME="${DATABRICKS_HOST_NAME}"
export DBSQLCLI_HTTP_PATH="${DATABRICKS_HTTP_PATH}"
export DBSQLCLI_ACCESS_TOKEN="${DATABRICKS_ACCESS_TOKEN}"

config_json_template='{
   "address": "$DATABRICKS_HOST_NAME",
   "http_path": "$DATABRICKS_HTTP_PATH",
   "catalog_name": "$DATABRICKS_CATALOG",
	 "schema_name": "some-schema",
	 "advanced": { "updateDelay": "0s" },
   "credentials": {
			"auth_type": "PAT",
			"personal_access_token": "$DATABRICKS_ACCESS_TOKEN"
		}
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
  }
]'

export CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"
export RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"
