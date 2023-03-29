#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

# TODO(johnny): This mostly works because deleting bindings also drops the tables.
# But, this script should nuke it from orbit and ensure we get back to a clean state.
