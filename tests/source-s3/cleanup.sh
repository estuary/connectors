#!/bin/bash

set -e

docker compose -f source-s3/docker-compose.yaml down -v
