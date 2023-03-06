#!/bin/bash
set -e

if [ $# -ne 1 ]
then
    echo "execution using: $0 <test-output-jsonl-dir>"
    exit 1
fi

# The relative path to ${TEST_DIR} to store final results.
result_dir="$1"

# Wait for enough long to have data available for fetching.
# TODO(johnny): gross.
sleep 2

function exportToJsonl() {
	projections=${2:-"{'_meta':0}"}
	docker exec \
    materialize-mongodb-mongo-1 mongosh test \
		--username flow \
		--password flow \
		--json=canonical \
		--quiet \
		--eval="db.$1.find({}, $projections).toArray()"  | jq -c '.[]'
}

exportToJsonl "Simple" > "${TEST_DIR}/${result_dir}/simple.jsonl"
exportToJsonl "duplicate_keys_standard" > "${TEST_DIR}/${result_dir}/duplicated-keys-standard.jsonl"
exportToJsonl "duplicate_keys_delta" "{'_meta':0,'_id':0}" > "${TEST_DIR}/${result_dir}/duplicated-keys-delta.jsonl"
