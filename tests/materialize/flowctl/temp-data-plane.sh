#!/bin/bash
#
# This script runs inside Flow docker container to start a temp docker container.

set -e
echo "start temp-data-plane.sh"
flowctl temp-data-plane \
    --sigterm \
    --tempdir "${TEST_DIR}" \
    --log.level debug