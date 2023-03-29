#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

docker compose --file  ./materialize-mongodb/docker-compose.yaml down || true