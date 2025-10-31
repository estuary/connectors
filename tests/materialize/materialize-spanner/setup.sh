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
      "table": "duplicate keys @ with spaces",
      "delta_updates": true
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
      "table": "symbols"
    },
    "source": "${TEST_COLLECTION_SYMBOLS}",
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
      "table": "string_escaped_key"
    },
    "source": "${TEST_COLLECTION_STRING_ESCAPED_KEY}"
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
export SPANNER_PROJECT_ID="$(echo $CONNECTOR_CONFIG | jq -r .project_id)"
export SPANNER_INSTANCE_ID="$(echo $CONNECTOR_CONFIG | jq -r .instance_id)"
export SPANNER_DATABASE="$(echo $CONNECTOR_CONFIG | jq -r .database)"

# Set credentials for gcloud if provided
export SPANNER_SERVICE_ACCOUNT_JSON="$(echo $CONNECTOR_CONFIG | jq -r '.credentials.service_account_json')"
if [ -n "$SPANNER_SERVICE_ACCOUNT_JSON" ]; then
  # Create a temporary service account key file for authentication
  export SPANNER_KEY_FILE=$(mktemp)
  echo "$SPANNER_SERVICE_ACCOUNT_JSON" > "$SPANNER_KEY_FILE"
  export GOOGLE_APPLICATION_CREDENTIALS="$SPANNER_KEY_FILE"
fi

export RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"
