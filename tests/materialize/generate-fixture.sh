#!/bin/bash

# Generate large-scale performance fixtures dynamically based on RESOURCES_CONFIG
# This script is a wrapper around the Python implementation for high performance

set -o errexit
set -o pipefail
set -o nounset

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Default to 100K documents if not specified
export PERF_DOC_COUNT=${PERF_DOC_COUNT:-100000}

# Get the flow.json file path (should be in TEMP_DIR)
FLOW_JSON=${1:-}
OUTPUT_FIXTURE=${2:-}

if [[ -z "${FLOW_JSON}" ]] || [[ -z "${OUTPUT_FIXTURE}" ]]; then
    echo "Usage: generate-fixture.sh <flow.json> <output-fixture.json>" >&2
    exit 1
fi

if [[ ! -f "${FLOW_JSON}" ]]; then
    echo "Error: Flow file ${FLOW_JSON} not found" >&2
    exit 1
fi

# Call the Python implementation for fast generation
exec python3 "${SCRIPT_DIR}/generate-fixture.py" "${FLOW_JSON}" "${OUTPUT_FIXTURE}"
