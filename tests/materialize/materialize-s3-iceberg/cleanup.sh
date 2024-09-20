#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

docker compose -f materialize-s3-iceberg/docker-compose.yaml down -v
aws s3 rm $S3_DATA_URI --recursive
