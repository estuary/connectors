#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

config_json_template='{
    "token": "${MOTHERDUCK_TOKEN}",
    "database": "${MOTHERDUCK_DATABASE}",
    "schema": "${MOTHERDUCK_SCHEMA}",
    "bucket": "${MOTHERDUCK_BUCKET}",
    "awsAccessKeyId": "${AWS_ACCESS_KEY_ID}",
    "awsSecretAccessKey": "${AWS_SECRET_ACCESS_KEY}",
    "region": "${AWS_REGION}"
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

STORAGE_TYPE="${STORAGE_TYPE:-s3}"
export CONNECTOR_CONFIG="$(decrypt_config $CONNECTOR_TEST_DIR/config.${STORAGE_TYPE}.yaml)"
export MOTHERDUCK_DATABASE="$(echo $CONNECTOR_CONFIG | jq -r .database)"
export MOTHERDUCK_SCHEMA="$(echo $CONNECTOR_CONFIG | jq -r .schema)"
export motherduck_token="$(echo $CONNECTOR_CONFIG | jq -r .token)"

export RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"
