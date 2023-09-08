#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

function exportToJsonl() {
	projections=${2:-"{'_meta':0}"}
	docker exec \
		materialize-mongodb-mongo-1 mongosh test \
		--username flow \
		--password flow \
		--json=canonical \
		--quiet \
		--eval="db.$1.find({}, $projections).toArray()" | jq -S "{ index: \"$1\", rows: . }"
}

exportToJsonl "Simple"
exportToJsonl "duplicate_keys_standard"
exportToJsonl "duplicate_keys_delta" "{'_meta':0,'_id':0}"
