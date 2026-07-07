#!/bin/bash
#
# Runs the runtime-v2 multi-shard "scale-out" test for a connector, driving it
# with two shards across two sessions via `flowctl raw preview-next`. The
# session boundary is the essential part of the test: runtime v2 halts a
# session after its final commit WITHOUT running the post-commit Acknowledge,
# so the second session must recover the consolidated connector state and the
# primary shard must replay every shard's staged work.
#
# Requirements:
#   - flowctl (a build including `raw preview-next`) on $PATH
#   - sops + GCP credentials able to decrypt the connector's testdata config
#   - Flow read access to the source collection, via a `FLOW_AUTH_TOKEN`
#     environment variable (CI) or an existing `flowctl auth login` profile
#     (local development)
#
# Usage: CONNECTOR=materialize-databricks tests/scale-out/run.sh

set -o errexit
set -o pipefail
set -o nounset

CONNECTOR="${CONNECTOR:?must specify CONNECTOR}"
ROOT_DIR="$(git rev-parse --show-toplevel)"
TEST_DIR="${ROOT_DIR}/tests/scale-out/${CONNECTOR}"

if [[ ! -d "${TEST_DIR}" ]]; then
    echo "no scale-out test exists for ${CONNECTOR}" && exit 1
fi

WORKDIR="$(mktemp -d)"
export WORKDIR

# setup.sh may define a test_cleanup function to remove destination resources.
function cleanup() {
    if declare -f test_cleanup > /dev/null; then
        test_cleanup || true
    fi
    rm -rf "${WORKDIR}"
}
trap cleanup EXIT

function bail() {
    echo "$@" 1>&2
    exit 1
}

command -v flowctl > /dev/null || bail "flowctl must be available via PATH"
flowctl raw preview-next --help > /dev/null 2>&1 ||
    bail "the flowctl on PATH does not support 'raw preview-next'"
if [[ "${CI:-}" == "true" && -z "${FLOW_AUTH_TOKEN:-}" ]]; then
    bail "FLOW_AUTH_TOKEN must be set in CI: a Flow access or refresh token" \
        "for an account with read access to the demo/ collections"
fi

echo "building ${CONNECTOR}"
(cd "${ROOT_DIR}" && go build -o "${WORKDIR}/connector" "./${CONNECTOR}")

# setup.sh writes ${WORKDIR}/local.flow.yaml (and its config) and
# exports TASK_NAME.
source "${TEST_DIR}/setup.sh"

echo "running preview-next for ${TASK_NAME} with 2 shards across 2 sessions"
# INFO-level tracing is load-bearing: the assertions below match against the
# connector's ops log lines, which are suppressed at the default level.
RUST_LOG="${RUST_LOG:-h2=info,info}" flowctl raw preview-next \
    --source "${WORKDIR}/local.flow.yaml" \
    --name "${TASK_NAME}" \
    --shards 2 \
    --sessions 1,1 \
    --timeout 600s \
    2> "${WORKDIR}/preview.stderr" ||
    (cat "${WORKDIR}/preview.stderr" && bail "preview-next failed")

# The connector must not have logged any errors.
if grep -q "ERROR" "${WORKDIR}/preview.stderr"; then
    grep -B2 -A8 "ERROR" "${WORKDIR}/preview.stderr"
    bail "preview-next logged errors"
fi

# Session two must have replayed session one's committed-but-unapplied
# transaction: the primary shard commits recovered documents to the
# destination as part of its recovery Acknowledge.
grep -q "started materialization recovery commit" "${WORKDIR}/preview.stderr" ||
    (tail -50 "${WORKDIR}/preview.stderr" && bail "no recovery commit was observed in session two")
grep -q "committing documents for resource" "${WORKDIR}/preview.stderr" ||
    (tail -50 "${WORKDIR}/preview.stderr" && bail "no documents were committed to the destination")

# verify.sh asserts on destination contents and cleans up test resources.
source "${TEST_DIR}/verify.sh"

echo "scale-out test for ${CONNECTOR} passed"
