#!/bin/bash
#
# Smoke test of a connector's built Docker image: fetch its spec and check that
# the result is valid JSON. This detects completely broken images, and is the
# only image-level coverage for connectors whose real tests drive the connector
# as a local command or an in-process driver rather than as a container.
#
# The connector name is read from the CONNECTOR env variable and the image tag
# from VERSION. Both are required.

set -o errexit
set -o pipefail
set -o nounset

command -v flowctl >/dev/null 2>&1 || {
  echo >&2 "flowctl must be available via PATH, aborting."
  exit 1
}

function bail() {
  echo "$@" 1>&2
  exit 1
}

test -n "${CONNECTOR}" || bail "must specify CONNECTOR env variable"
test -n "${VERSION}" || bail "must specify VERSION env variable"

CONNECTOR_IMAGE="ghcr.io/estuary/${CONNECTOR}:${VERSION}"
echo "smoke testing image: '${CONNECTOR_IMAGE}'"

cat <<EOF | flowctl raw spec --source - | jq -cM >/dev/null || bail "failed to fetch spec from ${CONNECTOR_IMAGE}"
{
  "captures": {
    "tests/${CONNECTOR}/spec": {
      "endpoint": {"connector": {"image": "${CONNECTOR_IMAGE}", "config": {}}},
      "bindings": []
    }
  }
}
EOF

echo "===========
Smoke Test Passed
==========="
