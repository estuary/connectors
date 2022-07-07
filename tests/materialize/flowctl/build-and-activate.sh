#!/bin/bash
#
set -e

echo "building and activating the testing catalog via build-and-activate.sh"

flowctl api build \
  --directory "${TEST_DIR}"/builds \
  --build-id "${BUILD_ID}" \
  --source "file://${TEST_DIR}/${CATALOG}" \
  --network host \
  --log.level debug

flowctl api activate \
  --build-id "${BUILD_ID}" \
  --network host \
  --log.level debug \
  --all
