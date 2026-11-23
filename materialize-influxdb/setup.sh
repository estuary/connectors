#!/bin/bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

docker network inspect flow-test >/dev/null 2>&1 || docker network create flow-test
docker compose --project-name materialize-influxdb-tests --file "$ROOT_DIR/materialize-influxdb/docker-compose.yaml" down --volumes
docker compose --project-name materialize-influxdb-tests --file "$ROOT_DIR/materialize-influxdb/docker-compose.yaml" up --detach --wait --wait-timeout 180
