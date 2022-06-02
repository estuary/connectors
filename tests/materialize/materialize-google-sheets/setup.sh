#!/bin/bash

set -e

# This spreadsheet lives under the Estuary org and is shared with our CI service account
# and engineering@. If you're an engineer, feel free to add additional service accounts
# as needed for your own testing:
# https://docs.google.com/spreadsheets/d/1aki_PfFU-RCXCvm4-U0O4QoElZBIC7F9lfR-RBG0CTc/edit#gid=0
export SPREADSHEET_ID="1aki_PfFU-RCXCvm4-U0O4QoElZBIC7F9lfR-RBG0CTc"
export GCP_SERVICE_ACCOUNT_KEY_QUOTED=$(echo ${GCP_SERVICE_ACCOUNT_KEY} | jq 'tojson')

config_json_template='{
    "googleCredentials": ${GCP_SERVICE_ACCOUNT_KEY_QUOTED},
    "spreadsheetId": "${SPREADSHEET_ID}"
}'

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

CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"
export CONNECTOR_CONFIG

RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"
export RESOURCES_CONFIG