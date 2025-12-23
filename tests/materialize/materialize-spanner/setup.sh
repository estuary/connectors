#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

standard_resources_json_template='[
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
  }
]'

perf_resources_json_template='[
  {
    "resource": {
      "table": "perf_simple"
    },
    "source": "${TEST_COLLECTION_PERF_SIMPLE}"
  },
  {
    "resource": {
      "table": "perf_uuid_key"
    },
    "source": "${TEST_COLLECTION_PERF_UUID_KEY}"
  }
]'

export CONNECTOR_CONFIG="$(decrypt_config $CONNECTOR_TEST_DIR/config.yaml)"

# Use performance bindings if PERF_DOC_COUNT is set, otherwise use standard test bindings
if [[ -n "${PERF_DOC_COUNT:-}" ]]; then
  echo "PERF_DOC_COUNT is set (${PERF_DOC_COUNT}), using performance test bindings only"
  export RESOURCES_CONFIG="$(echo "$perf_resources_json_template" | envsubst | jq -c)"
else
  export RESOURCES_CONFIG="$(echo "$standard_resources_json_template" | envsubst | jq -c)"
fi
