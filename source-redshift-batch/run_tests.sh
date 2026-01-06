#!/bin/bash
DIR=$(realpath $(dirname "$0"))
CONNECTOR=$(basename "$DIR")

export TEST_DATABASE="${TEST_DATABASE:-yes}"

cd $DIR/.. && go test -short -failfast -timeout=0 -v ./$CONNECTOR/... "$@"
