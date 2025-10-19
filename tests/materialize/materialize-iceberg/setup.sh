#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

export TABLE_SUFFIX=$(head -c 12 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 8 | tr '[:upper:]' '[:lower:]')

resources_json_template='[
  {
    "resource": {
      "table": "simple_${TABLE_SUFFIX}"
    },
    "source": "${TEST_COLLECTION_SIMPLE}"
  },
  {
    "resource": {
      "table": "duplicate_keys_standard_${TABLE_SUFFIX}"
    },
    "source": "${TEST_COLLECTION_DUPLICATED_KEYS}"
  },
  {
    "resource": {
      "table": "multiple_types_${TABLE_SUFFIX}"
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
      "table": "formatted_strings_${TABLE_SUFFIX}"
    },
    "source": "${TEST_COLLECTION_FORMATTED_STRINGS}",
    "fields": {
      "recommended": true
    }
  },
  {
    "resource": {
      "table": "deletions_${TABLE_SUFFIX}"
    },
    "source": "${TEST_COLLECTION_DELETIONS}"
  },
  {
    "resource": {
      "table": "binary_key_${TABLE_SUFFIX}"
    },
    "source": "${TEST_COLLECTION_BINARY_KEY}"
  },
  {
    "resource": {
      "table": "string_escaped_key_${TABLE_SUFFIX}"
    },
    "source": "${TEST_COLLECTION_STRING_ESCAPED_KEY}"
  }
]'

# Return the first non-null/non-empty-string value in a list of paths.
select_first() {
	jq -r 'first($ARGS.positional[] as $f | getpath($f / ".") | strings | select(. != ""))' --args "$@"
}

# Extract the signing name from the first component of the host.
#
# Example:
# ```
# >>> signing_name glue.foo.bar
# glue
# ```
signing_name() {
	read -r tmp
	tmp=${tmp#*//}
	echo ${tmp%%.*}
}

CATALOG_TYPE="${CATALOG_TYPE:-rest}"
export CONNECTOR_CONFIG="$(decrypt_config $CONNECTOR_TEST_DIR/config.${CATALOG_TYPE}.yaml)"
export CATALOG_URL="$(echo $CONNECTOR_CONFIG | jq -r .url)"
export WAREHOUSE="$(echo $CONNECTOR_CONFIG | jq -r .warehouse)"
export NAMESPACE="$(echo $CONNECTOR_CONFIG | jq -r .namespace)"
export CATALOG_CREDENTIAL="$(echo $CONNECTOR_CONFIG | select_first credentials.credential catalog_authentication.credential)"
export CATALOG_SCOPE="$(echo $CONNECTOR_CONFIG | select_first credentials.scope catalog_authentication.scope)"
export CATALOG_AWS_ACCESS_KEY_ID="$(echo $CONNECTOR_CONFIG | select_first credentials.aws_access_key_id catalog_authentication.aws_access_key_id)"
export CATALOG_AWS_SECRET_ACCESS_KEY="$(echo $CONNECTOR_CONFIG | select_first credentials.aws_secret_access_key catalog_authentication.aws_secret_access_key)"
export CATALOG_REGION="$(echo $CONNECTOR_CONFIG | select_first credentials.aws_region catalog_authentication.region)"
export CATALOG_AWS_SIGNING_NAME="$(echo $CONNECTOR_CONFIG | jq -r .url | signing_name)"

export RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"

for var in CATALOG_CREDENTIAL CATALOG_SCOPE CATALOG_AWS_ACCESS_KEY_ID CATALOG_AWS_SECRET_ACCESS_KEY CATALOG_REGION CATALOG_AWS_SIGNING_NAME; do
    [ "${!var}" = "null" ] && eval "$var=''"
done

ICEBERG_HELPER_CMD="go run $(git rev-parse --show-toplevel)/materialize-iceberg/cmd/iceberg_helper --catalog-url ${CATALOG_URL} --warehouse ${WAREHOUSE}"

[ -n "$CATALOG_CREDENTIAL" ] && ICEBERG_HELPER_CMD+=" --client-credential ${CATALOG_CREDENTIAL}"
[ -n "$CATALOG_SCOPE" ] && ICEBERG_HELPER_CMD+=" --scope ${CATALOG_SCOPE}"

if [ -n "$CATALOG_AWS_ACCESS_KEY_ID" ]; then
    ICEBERG_HELPER_CMD+=" --signing-name ${CATALOG_AWS_SIGNING_NAME} --aws-access-key-id ${CATALOG_AWS_ACCESS_KEY_ID} --aws-secret-access-key ${CATALOG_AWS_SECRET_ACCESS_KEY} --region ${CATALOG_REGION}"
fi

export ICEBERG_HELPER_CMD
