#!/bin/bash
set -e

if [ $# -ne 1 ]; then
  echo "execution using: $0 <test-output-jsonl-dir>"
  exit 1
fi

# The relative path to ${TEST_DIR} to store final results.
result_dir="$1"

# Wait for enough long to have data available for fetching.
sleep 20

function exportToJsonl() {
  go run "${TEST_BASE_DIR}"/materialize-redshift/fetch-data.go "$1"
}

exportToJsonl "simple" >"${TEST_DIR}/${result_dir}/simple.jsonl"
exportToJsonl "duplicate_keys_standard" >"${TEST_DIR}/${result_dir}/duplicated-keys-standard.jsonl"
exportToJsonl "duplicate_keys_delta" >"${TEST_DIR}/${result_dir}/duplicated-keys-delta.jsonl"
exportToJsonl "duplicate_keys_delta_exclude_flow_doc" >"${TEST_DIR}/${result_dir}/duplicated-keys-delta-exclude-flow-doc.jsonl"
exportToJsonl "multiple_types" >"${TEST_DIR}/${result_dir}/multiple-data-types.jsonl"
exportToJsonl "formatted_strings" >"${TEST_DIR}/${result_dir}/formatted-strings.jsonl"
