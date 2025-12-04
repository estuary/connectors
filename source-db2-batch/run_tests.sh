#!/bin/bash
DIR=$(realpath $(dirname "$0"))
CONNECTOR=$(basename "$DIR")

# Set up IBM DB2 clidriver environment
export GOPATH=${GOPATH:-$(go env GOPATH)}
export IBM_DB_HOME=${IBM_DB_HOME:-$GOPATH/pkg/mod/github.com/ibmdb/clidriver}
export CGO_CFLAGS=-I$IBM_DB_HOME/include
export CGO_LDFLAGS=-L$IBM_DB_HOME/lib
export LD_LIBRARY_PATH=$IBM_DB_HOME/lib:$LD_LIBRARY_PATH

# Enable database tests
export TEST_DATABASE=yes

cd $DIR/.. && go test -timeout=10m -count=1 ./$CONNECTOR/... "$@"
