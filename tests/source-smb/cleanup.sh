#!/bin/bash

set -e

docker compose -f source-smb/docker-compose.yaml down -v
