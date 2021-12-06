#!/bin/bash
#
# This script runs inside Flow docker container to start a temp docker container.

set -e

flowctl temp-data-plane \
    --sigterm \
    --tempdir ${TEST_DIR}