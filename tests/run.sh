#!/bin/bash
#
# This script executes an end-to-end integration test of a connector's built
# Docker image. The connector name is read from the CONNECTOR env variable, and
# the image tag is read from VERSION. Both of those are required.
#
# The test executes a connector-specific setup script, then runs the connector
# under `flowctl raw preview-next` and compares the captured documents against
# a checked-in snapshot. Re-run with UPDATE_SNAPSHOTS=true to regenerate the
# snapshot after an intentional change.

set -o errexit
set -o pipefail
set -o nounset

command -v flowctl >/dev/null 2>&1 || {
  echo >&2 "flowctl must be available via PATH, aborting."
  exit 1
}

ROOT_DIR="$(git rev-parse --show-toplevel)"
cd "$ROOT_DIR"

function bail() {
  echo "$@" 1>&2
  exit 1
}

test -n "$CONNECTOR" || bail "must specify CONNECTOR env variable"
test -n "$VERSION" || bail "must specify VERSION env variable"

UPDATE_SNAPSHOTS="${UPDATE_SNAPSHOTS:-}"

# Connector image to use. Export to make it available to `envsubst`
export CONNECTOR_IMAGE="ghcr.io/estuary/${CONNECTOR}:${VERSION}"
echo "testing connector: '$CONNECTOR'"

# Directory under which the test runs.
TESTDIR=".build/tests/${CONNECTOR}"
# Post-templating catalog source processed by the test.
CATALOG_SOURCE="${TESTDIR}/test.flow.yaml"
# Checked-in snapshot of the documents the capture is expected to produce.
SNAPSHOT="tests/${CONNECTOR}/snapshot.jsonl"
# Actual capture output, normalized for comparison against ${SNAPSHOT}.
ACTUAL="${TESTDIR}/actual.jsonl"

# Ensure we start with an empty dir. Remove it, if it exists already.
if [[ -d "${TESTDIR}" ]]; then
  rm -r ${TESTDIR}
fi
mkdir -p "${TESTDIR}"

# Map to an absolute directory.
export TESTDIR=$(realpath ${TESTDIR})

# Execute test-specific setup steps.
echo -e "\nexecuting setup"
source "tests/${CONNECTOR}/setup.sh" || bail "${CONNECTOR}/setup.sh failed"
if [[ -z "$RESOURCE" ]]; then
  bail "setup did not set RESOURCE"
fi

if [[ -z "$CONNECTOR_CONFIG" ]]; then
  bail "setup did not set CONNECTOR_CONFIG"
fi

TEST_STATUS="Test Failed"
function test_shutdown() {
  ./tests/${CONNECTOR}/cleanup.sh
  echo -e "===========\n${TEST_STATUS}\n==========="
}
trap "test_shutdown" EXIT

# Get the spec from the connector and ensure it's valid json.
cat >"$TESTDIR/spec.yaml" <<EOF
captures:
  tests/${CONNECTOR}/from-source:
    endpoint:
      connector:
        image: "${CONNECTOR_IMAGE}"
        config: ${CONNECTOR_CONFIG}
    bindings: []
EOF

flowctl raw spec --source "$TESTDIR/spec.yaml" | jq -cM || bail "failed to validate spec"

# A collection key may only be a single type, so the id property's type is a
# per-connector variable rather than a union. Connectors whose parser yields
# strings for every column -- anything reading CSV -- set this to 'string' in
# their setup.sh. Default to integer so the others needn't export it.
export ID_TYPE="${ID_TYPE:-integer}"

# Verify discover works. The output isn't asserted against, but a connector
# which crashes or emits malformed bindings fails here.
flowctl raw discover --network flow-test --source $TESTDIR/spec.yaml -o json --emit-raw \
  >${TESTDIR}/discover_output.json || bail "Discover failed."

# Generate the test-specific catalog source.
cat tests/template.flow.yaml | envsubst >"${CATALOG_SOURCE}"

# Run the capture. A single session of one transaction, with a delay which
# raises that transaction's minimum duration, so it batches everything the
# connector produces within the delay window and then stops.
flowctl raw preview-next \
  --source "${CATALOG_SOURCE}" \
  --network "flow-test" \
  --sessions 1 \
  --delay 30s \
  >"${TESTDIR}/raw.jsonl" || bail "Capture failed."

# Normalize the captured documents. Everything lands in one transaction, so the
# order documents are emitted in isn't guaranteed across a 30s window; sort so
# the comparison is order-independent. Connectors which embed a per-run value in
# their documents sanitize it here.
function normalize() {
  case "${CONNECTOR}" in
  source-kinesis)
    # Kinesis assigns each record a sequence number which encodes the time it
    # was written, so it differs on every run. The rest of _meta is stable:
    # partition keys are fixed by setup.sh, and the shard follows from them.
    jq -cM 'if (.[1] | type) == "object" and ((.[1]._meta.sequence_number? | type) == "string")
            then .[1]._meta.sequence_number = "SEQUENCE_NUMBER"
            else . end'
    ;;
  source-gcs)
    # The bucket is created fresh with a random name on each run, and filesource
    # keys documents on /_meta/file, which is "<bucket>/<prefix>/<name>". Replace
    # the bucket component with a constant.
    jq -cM --arg bucket "${TEST_STREAM}" \
      'if (.[1] | type) == "object" and ((.[1]._meta.file? | type) == "string")
       then .[1]._meta.file |= sub("^" + $bucket + "/"; "TESTBUCKET/")
       else . end'
    ;;
  *)
    cat
    ;;
  esac
}

normalize <"${TESTDIR}/raw.jsonl" | LC_ALL=C sort >"${ACTUAL}"

if [[ -n "${UPDATE_SNAPSHOTS}" ]]; then
  cp "${ACTUAL}" "${SNAPSHOT}"
  echo "updated snapshot ${SNAPSHOT}"
else
  test -f "${SNAPSHOT}" || bail "missing snapshot ${SNAPSHOT}, re-run with UPDATE_SNAPSHOTS=true to create it"
  echo "-- RUNNING DIFF"
  diff --suppress-common-lines --side-by-side "${ACTUAL}" "${SNAPSHOT}" || bail "Test Failed"
fi

# Will be printed by the shutdown trap *after* any shutdown logging from flowctl
TEST_STATUS="Test Passed"
