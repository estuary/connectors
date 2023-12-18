#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset
set -x

ROOTDIR="$(git rev-parse --show-toplevel)"
CONNECTOR=$1

poetry --directory ${ROOTDIR}/${CONNECTOR} install --no-root
poetry --directory ${ROOTDIR}/${CONNECTOR} shell