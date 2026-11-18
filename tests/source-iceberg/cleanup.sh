#!/bin/bash

set -e

docker compose -f source-iceberg/docker-compose.yaml down -v --remove-orphans
