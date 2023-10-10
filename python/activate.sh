#!/bin/bash
# Description: switch which connector's python configuration is setup for development

set -o errexit
set -o pipefail
set -o nounset

ROOTDIR="$(git rev-parse --show-toplevel)"
CONNECTOR=$1

# Unlink previous connector.
rm ${ROOTDIR}/pyproject.toml 2> /dev/null || true
rm ${ROOTDIR}/poetry.lock 2> /dev/null || true

# Link activated connector.
ln ${ROOTDIR}/${CONNECTOR}/pyproject.toml ${ROOTDIR}/pyproject.toml
ln ${ROOTDIR}/${CONNECTOR}/poetry.lock ${ROOTDIR}/poetry.lock

cd ${ROOTDIR} && poetry install

# check if other required tools are installed
if ! command -v sops &> /dev/null; then
  echo -e "\nsops could not be found, install with:\n\n   brew install sops"
  exit 1
fi