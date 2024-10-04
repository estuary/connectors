#!/bin/bash

set -e

function clearNamespace() {
    go run ${TEST_DIR}/materialize-pinecone/fetch-data.go --clear-namespace "$1"
}

clearNamespace "simple"
clearNamespace "duplicated-keys"
clearNamespace "multiple-types"
