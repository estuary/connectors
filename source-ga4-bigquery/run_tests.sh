#!/bin/bash
DIR=$(realpath $(dirname "$0"))
CONNECTOR=$(basename "$DIR")

cd $DIR/.. && go test -timeout=10m -count=1 ./$CONNECTOR/... "$@"
