#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

docker compose --file ./materialize-sqlserver/docker-compose.yaml down -v || true
