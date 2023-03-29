#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

docker rm -f "${LOCALSTACK_CONTAINER_NAME}"