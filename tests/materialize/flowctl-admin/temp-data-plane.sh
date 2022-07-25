#!/bin/bash
#

set -e
echo "starting a temp data plane via temp-data-plane.sh"

flowctl-admin temp-data-plane \
    --sigterm \
    --tempdir "${TEST_DIR}" \
    --network host \
    --log.level debug
