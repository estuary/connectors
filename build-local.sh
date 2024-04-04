#!/bin/bash
if [[ $# < 1 ]]; then
    echo "This script builds \"<connector_name>:local\" images for testing."
    echo "Usage: build-local.sh <connector_name> <Dockerfile>"
    exit 1
fi

if [[ $1 == source-* ]]; 
then
  CONNECTOR_TYPE=capture
elif [[ $1 == materialize-* ]];
then
  CONNECTOR_TYPE=materialization
fi

if [ -z "$2" ]
then
  DOCKERFILE="$1"/Dockerfile
else
  DOCKERFILE=$2
fi

docker buildx build \
  --cache-to=type=local,dest=.docker-cache \
  --cache-from=type=local,src=.docker-cache \
  --platform linux/amd64 \
  --build-arg CONNECTOR_NAME="$1" \
  --build-arg CONNECTOR_TYPE="$CONNECTOR_TYPE" \
  --build-arg="USAGE_RATE=1.0" \
  --load \
  -t ghcr.io/estuary/"$1":local \
  -f "$DOCKERFILE" \
  .
