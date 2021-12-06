#!/bin/bash
#
# This script builds and activates the test catalog.
set -e

echo "start build-and-activate.sh"


flowctl api build \
  --directory "${TEST_DIR}"/build \
  --build-id "${BUILD_ID}" \
  --source "file://${TEST_DIR}/${CATALOG}" \
  --log.level debug

flowctl api activate \
  --build-id "${BUILD_ID}" \
  --log.level debug \
  --all