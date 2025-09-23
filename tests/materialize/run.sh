#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

command -v flowctl >/dev/null 2>&1 || { echo >&2 "flowctl must be available via PATH, aborting."; exit 1; }
test -n "${CONNECTOR}" || bail "must specify CONNECTOR env variable"
test -n "${VERSION}" || bail "must specify VERSION env variable"

ROOT_DIR="$(git rev-parse --show-toplevel)"
cd "$ROOT_DIR"

# Test common materialization packages. This is repeated for each
# materialization during CI runs which is quite redundant, but these tests are
# fast so it doesn't really matter. This should be optimized later with other CI
# improvements.

cd $ROOT_DIR/go && go test -v ./...
cd $ROOT_DIR/filesink && go test -v ./...
cd $ROOT_DIR/materialize-boilerplate && go test -v ./...

# Run the specific connector tests.
cd $ROOT_DIR/$CONNECTOR
go test -v ./...

# Verify that the built image at least works enough to run the spec command.
# This is intended as a smoke test to detect completely broken images, since the
# connector integration tests run with a local command, rather than a built
# container image.
echo "{\"materializations\":{\"acmeCo/tests/spec\":{\"endpoint\":{\"connector\":{\"image\":\"ghcr.io/estuary/${CONNECTOR}:${VERSION}\",\"config\":{}}},\"bindings\":[]}}}" | flowctl raw spec --source - | jq >/dev/null 2>&1
