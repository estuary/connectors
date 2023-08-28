#!/bin/bash

set -e

docker compose -f materialize-dynamodb/docker-compose.yaml down -v
