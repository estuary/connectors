#!/bin/bash

set -e

docker compose -f source-sftp/docker-compose.yaml down -v
