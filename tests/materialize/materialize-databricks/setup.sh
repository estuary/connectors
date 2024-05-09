#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

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
  },
  {
    "resource": {
      "table": "unsigned_bigint"
    },
    "source": "${TEST_COLLECTION_UNSIGNED_BIGINT}",
    "fields": {
      "recommended": true,
      "include": {
        "unsigned_bigint": {"DDL": "DECIMAL(20)"}
      }
    }
  }
]'

export CONNECTOR_CONFIG="$(decrypt_config ${TEST_DIR}/${CONNECTOR}/config.yaml)"
export RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"

export DBSQLCLI_HOST_NAME="$(echo $CONNECTOR_CONFIG | jq -r .address)"
export DBSQLCLI_HTTP_PATH="$(echo $CONNECTOR_CONFIG | jq -r .http_path)"
export DBSQLCLI_ACCESS_TOKEN="$(echo $CONNECTOR_CONFIG | jq -r .credentials.personal_access_token)"
export DATABRICKS_CATALOG="$(echo $CONNECTOR_CONFIG | jq -r .catalog_name)"
export DATABRICKS_SCHEMA="$(echo $CONNECTOR_CONFIG | jq -r .schema)"
export DATABRICKS_ACCESS_TOKEN="$DBSQLCLI_ACCESS_TOKEN"
export DATABRICKS_HOST_NAME="$DBSQLCLI_HOST_NAME"
export DATABRICKS_HTTP_PATH="$DBSQLCLI_HTTP_PATH"
