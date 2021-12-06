#!/bin/bash
#
# This script builds and activates the test catalog.

set -e

flowctl api build --directory ${TEST_DIR}/build --build-id ${BUILD_ID} --source ${TEST_DIR}/${CATALOG} --ts-package
flowctl api activate --build-id ${BUILD_ID} --all