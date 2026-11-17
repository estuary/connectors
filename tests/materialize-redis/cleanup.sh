#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

docker compose --file materialize-redis/docker-compose.yaml down --volumes
