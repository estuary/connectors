#!/bin/bash
DIR=$(realpath $(dirname "$0"))
CONNECTOR=$(basename "$DIR")

export TEST_DATABASE="${TEST_DATABASE:-yes}"

cd $DIR/.. && go test -timeout=0 -count=1 ./$CONNECTOR/... "$@"
