#!/bin/bash
#
# This script builds and activates the test catalog.

set -e
echo "start delete.sh"

flowctl api delete \
  --build-id ${BUILD_ID} \
  --log.level debug \
  --all