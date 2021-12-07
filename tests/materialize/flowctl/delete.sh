#!/bin/bash
#

set -e
echo "deleting the testing catalog via delete.sh"

flowctl api delete \
  --build-id ${BUILD_ID} \
  --log.level debug \
  --all