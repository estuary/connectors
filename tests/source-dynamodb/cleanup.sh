#!/bin/bash

set -e

docker compose -f source-dynamodb/docker-compose.yaml down -v
