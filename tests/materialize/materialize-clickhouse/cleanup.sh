#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

docker compose --file ./materialize-clickhouse/docker-compose.yaml down -v || true
