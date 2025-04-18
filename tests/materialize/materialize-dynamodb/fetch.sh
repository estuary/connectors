#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

function scanTable() {
  aws dynamodb --endpoint-url "$DYNAMODB_LOCAL_ENDPOINT" scan --table-name "$1" |
    jq "{ _table: \"$1\", rows: .Items }"
}

scanTable "simple"
scanTable "duplicated-keys-standard"
scanTable "multiple-types"
scanTable "formatted-strings"
