#!/bin/bash

set -e

docker compose -f materialize-kafka/docker-compose.yaml down -v
