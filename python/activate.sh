#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset
set -x

ROOTDIR="$(git rev-parse --show-toplevel)"
CONNECTOR=$1

poetry -C ${ROOTDIR}/${CONNECTOR} install
poetry -C ${ROOTDIR}/${CONNECTOR} shell