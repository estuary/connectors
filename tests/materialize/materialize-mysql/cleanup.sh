#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

# Nothing to do because deleting the materialization also cleans up its tables.

docker compose --file  ./materialize-mysql/docker-compose.yaml down || true
docker volume rm materialize-mysql_mysql_data || true
