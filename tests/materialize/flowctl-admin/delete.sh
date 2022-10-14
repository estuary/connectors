#!/bin/bash
#

set -e
echo "deleting the testing catalog via delete.sh"

flowctl-admin api delete \
  --build-id ${BUILD_ID} \
  --network host \
  --log.level info \
  --all
