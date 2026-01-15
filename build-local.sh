#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

name="$1"

if [[ -z "$name" ]]; then
    echo "This script builds \"<connector_name>:local\" images for testing."
    echo ""
    echo "Usage: build-local.sh <connector-name> [Dockerfile]"
    echo ""
    echo "Example: build-local.sh source-salesforce-native"
    exit 1
fi

# Determine connector type from name prefix
if [[ "$name" == source-* ]]; then
    type="capture"
elif [[ "$name" == materialize-* ]]; then
    type="materialize"
else
    echo "Invalid argument. Must start with 'source-' or 'materialize-'."
    exit 1
fi

# Detect Python vs Go/Rust connector and select Dockerfile
if [[ -f "${SCRIPT_DIR}/${name}/pyproject.toml" ]]; then
    # Python connector - use common Dockerfile
    dockerfile="${SCRIPT_DIR}/estuary-cdk/common.Dockerfile"
else
    # Go connector - use connector-specific Dockerfile
    if [[ -z "$2" ]]; then
        dockerfile="${name}/Dockerfile"
    else
        dockerfile="$2"
    fi
fi

docker buildx build \
    --cache-to=type=local,dest=.docker-cache \
    --cache-from=type=local,src=.docker-cache \
    --platform linux/amd64 \
    --build-arg CONNECTOR_NAME="$name" \
    --build-arg CONNECTOR_TYPE="$type" \
    --build-arg="USAGE_RATE=1.0" \
    --load \
    -t "ghcr.io/estuary/${name}:local" \
    -f "$dockerfile" \
    .
