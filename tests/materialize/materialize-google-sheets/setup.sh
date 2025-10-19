#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

resources_json_template='[
  {
    "resource": {
      "sheet": "Simple"
    },
    "source": "${TEST_COLLECTION_SIMPLE}"
  },
  {
    "resource": {
      "sheet": "duplicate_keys"
    },
    "source": "${TEST_COLLECTION_DUPLICATED_KEYS}"
  },
  {
    "resource": {
      "sheet": "Multiple Types"
    },
    "source": "${TEST_COLLECTION_MULTIPLE_DATATYPES}"
  }
]'

export CONNECTOR_CONFIG="$(decrypt_config $CONNECTOR_TEST_DIR/config.yaml)"
export SPREADSHEET_ID=$(echo $CONNECTOR_CONFIG | jq -r .spreadsheetUrl | sed -E 's#.*/d/([^/]+)/.*#\1#')
export GCP_SERVICE_ACCOUNT_KEY=$(echo $CONNECTOR_CONFIG | jq -r .credentials.credentials_json)

export RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"
