#!/bin/bash
#
# This script runs inside Flow docker container to start a temp docker container.

set -e

IFS=" " read -r -a args <<< "$1"
if [[ ${#args[@]} -ne 3 ]]; then
        echo "Usage: $0 <collection> <input-file-name> <output-file-name>"
fi

flowctl combine \
    --collection "${args[0]}" \
    --source "file://${TEST_DIR}/${CATALOG}" \
    --log.level=debug \
    < "${TEST_DIR}/${args[1]}" \
    > "${TEST_DIR}/${args[2]}"