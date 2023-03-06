#!/bin/bash

set -e

docker exec materialize-mongodb-mongo-1 mongosh test --username flow --password flow --eval="db.Simple.drop()"
docker exec materialize-mongodb-mongo-1 mongosh test --username flow --password flow --eval="db.duplicate_keys_standard.drop()"
docker exec materialize-mongodb-mongo-1 mongosh test --username flow --password flow --eval="db.duplicate_keys_delta.drop()"
docker exec materialize-mongodb-mongo-1 mongosh test --username flow --password flow --eval="db.flow_checkpoints.drop()"
