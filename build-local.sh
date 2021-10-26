#!/bin/bash
if [[ $# < 1 ]]; then
    echo "This script builds \"<connector_name>:local\" images for testing."
    echo "Usage: build-local.sh <connector_name>"
    exit 1
fi
docker build -t $1:local -f $1/Dockerfile .
