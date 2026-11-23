#!/bin/bash
set -euo pipefail

docker compose -f source-db2/docker-compose.yaml down --volumes
