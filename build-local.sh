#!/bin/bash
if [[ $# < 1 ]]; then
    echo "This script builds \"<connector_name>:local\" images for testing."
    echo "Usage: build-local.sh <connector_name>"
    exit 1
fi
docker buildx build \
  --cache-to=type=local,dest=.docker-cache \
  --cache-from=type=local,src=.docker-cache \
  --platform linux/amd64 \
  --load \
  -t $1:local \
  -f $1/Dockerfile \
  .
