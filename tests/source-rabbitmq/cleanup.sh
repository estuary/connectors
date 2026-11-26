#!/bin/bash

set -e

docker compose -f source-rabbitmq/docker-compose.yaml down -v
