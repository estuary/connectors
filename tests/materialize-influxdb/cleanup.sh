#!/bin/bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

docker compose --project-name materialize-influxdb-tests --file "$ROOT_DIR/materialize-influxdb/docker-compose.yaml" down --volumes
