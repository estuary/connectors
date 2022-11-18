#!/bin/bash
#

set -e
echo "starting a temp data plane via temp-data-plane.sh"

flowctl-go temp-data-plane \
    --sigterm \
    --network "flow-test" \
    --tempdir "${TEST_DIR}" \
    --log.level info
