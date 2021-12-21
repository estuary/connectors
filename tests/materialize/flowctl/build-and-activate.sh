#!/bin/bash
#
set -e

echo "building and activating the testing catalog via build-and-activate.sh"

flowctl api build \
  --directory "${TEST_DIR}"/build \
  --build-id "${BUILD_ID}" \
  --source "file://${TEST_DIR}/${CATALOG}" \
  --log.level debug

flowctl api activate \
  --build-id "${BUILD_ID}" \
  --log.level debug \
  --all