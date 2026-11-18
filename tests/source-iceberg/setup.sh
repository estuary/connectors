#!/bin/bash

set -e

export RESOURCE='{"namespace":"shop","table":"orders","interval":"PT5S"}'
export CONNECTOR_CONFIG='{
    "catalog_uri": "http://source-iceberg-db:8181",
    "namespaces": ["shop"],
    "s3": {
        "endpoint": "http://source-iceberg-storage:9000",
        "region": "us-east-1",
        "access_key_id": "test_key",
        "secret_access_key": "test_secret",
        "path_style_access": true
    }
}'

# tests/run.sh templates its catalog with envsubst, which some runners lack.
if ! command -v envsubst >/dev/null 2>&1; then
    export PATH="${PWD}/tests/source-iceberg/bin:${PATH}"
fi

docker network inspect flow-test >/dev/null 2>&1 || docker network create flow-test || return 1
docker compose -f source-iceberg/docker-compose.yaml down -v --remove-orphans || return 1
if ! docker compose -f source-iceberg/docker-compose.yaml up -d --wait --wait-timeout 180; then
    docker compose -f source-iceberg/docker-compose.yaml logs
    ./tests/source-iceberg/cleanup.sh
    return 1
fi

# Use the built image's dependencies and the same network as the capture.
if ! docker run --rm --network flow-test \
    --mount "type=bind,source=${PWD}/tests/source-iceberg,target=/fixture,readonly" \
    --entrypoint /opt/venv/bin/python \
    "${CONNECTOR_IMAGE}" /fixture/seed.py; then
    ./tests/source-iceberg/cleanup.sh
    return 1
fi
