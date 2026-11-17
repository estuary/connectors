#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

docker network inspect flow-test >/dev/null 2>&1 || docker network create flow-test
docker compose --file materialize-redis/docker-compose.yaml up --wait --wait-timeout 60
