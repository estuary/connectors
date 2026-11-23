#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose.yaml"

docker network inspect flow-test >/dev/null 2>&1 || docker network create flow-test
if ! docker compose -f "$COMPOSE_FILE" up --wait --wait-timeout 600; then
  docker compose -f "$COMPOSE_FILE" logs
  exit 1
fi
