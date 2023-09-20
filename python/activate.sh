#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

ROOTDIR="$(git rev-parse --show-toplevel)"
CONNECTOR=$1

# Unlink previous connector.
rm ${ROOTDIR}/pyproject.toml || true
rm ${ROOTDIR}/poetry.lock || true

# Link activated connector.
ln ${ROOTDIR}/${CONNECTOR}/pyproject.toml ${ROOTDIR}/pyproject.toml
ln ${ROOTDIR}/${CONNECTOR}/poetry.lock ${ROOTDIR}/poetry.lock

cd ${ROOTDIR} && poetry install