#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

chmod 400 materialize-mongodb/sample.key
docker compose -f materialize-mongodb/docker-compose.yaml up --detach

docker exec materialize-mongodb-mongo-1 \
	chown mongodb /etc/ssl/sample.key

set +e
# need to wait some seconds before the replication server is set up
sleep 5
docker exec materialize-mongodb-mongo-1 \
	mongosh \
	-u flow \
	-p flow \
	--eval 'rs.initiate()'

sleep 1

docker exec materialize-mongodb-mongo-1 \
	mongosh \
	-u flow \
	-p flow \
	--eval 'db.createUser({ user: "flow", pwd: "flow", roles: [{ role: "readWrite", db: "test" }] })'
set -e


config_json_template='{
   "address":  "materialize-mongodb-mongo-1",
   "database": "test",
   "password": "flow",
   "user":     "flow"
}'

resources_json_template='[
  {
    "resource": {
      "collection": "Simple"
    },
    "source": "${TEST_COLLECTION_SIMPLE}"
  },
  {
    "resource": {
      "collection": "duplicate_keys_standard"
    },
    "source": "${TEST_COLLECTION_DUPLICATED_KEYS}"
  },
  {
    "resource": {
      "collection": "duplicate_keys_delta",
      "delta_updates": true
    },
    "source": "${TEST_COLLECTION_DUPLICATED_KEYS}"
  }
]'

export CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"
export RESOURCES_CONFIG="$(echo "$resources_json_template" | envsubst | jq -c)"