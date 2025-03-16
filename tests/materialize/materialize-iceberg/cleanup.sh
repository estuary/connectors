#!/bin/bash

function purge() {
    ${ICEBERG_HELPER_CMD} --force purge ${NAMESPACE}."$1"_${TABLE_SUFFIX}
}

purge "simple"
purge "duplicate_keys_standard"
purge "multiple_types"
purge "formatted_strings"
purge "deletions"
purge "binary_key"
purge "string_escaped_key"
