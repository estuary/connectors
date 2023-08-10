#!/bin/bash
set -e

docker compose -f materialize-elasticsearch/docker-compose.yaml down -v
