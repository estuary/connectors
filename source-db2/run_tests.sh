#!/bin/bash
set -euo pipefail

DB2_DIR="$(cd "$(dirname "$0")" && pwd)"

# Use an already-installed clidriver, as in source-db2-batch.
export IBM_DB_HOME="${IBM_DB_HOME:-$(go env GOPATH)/pkg/mod/github.com/ibmdb/clidriver}"
export CGO_CFLAGS="-I$IBM_DB_HOME/include${CGO_CFLAGS:+ $CGO_CFLAGS}"
export CGO_LDFLAGS="-L$IBM_DB_HOME/lib${CGO_LDFLAGS:+ $CGO_LDFLAGS}"
export LD_LIBRARY_PATH="$IBM_DB_HOME/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

# Database tests remain opt-in via TEST_DATABASE=yes and TEST_DB2_PASSWORD.
cd "$DB2_DIR/.."
go test -count=1 ./source-db2/... "$@"
