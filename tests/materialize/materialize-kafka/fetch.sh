#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

docker run --tty --rm --network flow-test \
        edenhill/kcat:1.7.1 \
        -b materialize-kafka-db-1.flow-test:9092 \
        -t simple \
        -o beginning \
        -J \
        -q \
        -e | jq 'del(.ts)' | jq -s 'sort_by(.key)'
