#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

# Nothing to do because deleting the materialization also cleans up its tables.

docker compose --file  ./materialize-postgres/docker-compose.yaml down || true
docker volume rm materialize-postgres_postgres_data || true