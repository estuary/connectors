#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset
set -x

ROOTDIR="$(git rev-parse --show-toplevel)"
CONNECTOR=$1

poetry --directory ${ROOTDIR}/${CONNECTOR} install --no-root
poetry --directory ${ROOTDIR}/${CONNECTOR} add --group dev pre-commit ruff isort

poetry --directory ${ROOTDIR}/${CONNECTOR} run isort ${ROOTDIR}/${CONNECTOR} --profile black --skip __main__.py
poetry --directory ${ROOTDIR}/${CONNECTOR} run ruff format ${ROOTDIR}/${CONNECTOR}

git add ${ROOTDIR}/${CONNECTOR}
git commit -m "Format ${CONNECTOR} with ruff and isort" --no-verify
