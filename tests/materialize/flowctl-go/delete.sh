#!/bin/bash
#

set -e
echo "deleting the testing catalog via delete.sh"

flowctl-go api delete \
  --build-id ${BUILD_ID} \
  --network "flow-test" \
  --log.level info \
  --all
