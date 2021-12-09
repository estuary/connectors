#!/bin/bash
#

set -e
echo "starting a temp data plane via temp-data-plane.sh"

flowctl temp-data-plane \
    --sigterm \
    --tempdir "${TEST_DIR}" \
    --log.level debug