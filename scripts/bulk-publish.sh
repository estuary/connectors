#!/bin/sh
set -e
TMP="$(mktemp -d)"
DIR="$(dirname $0)"
NAME="$(basename $0 .sh)"
trap "rm -rf '$TMP'" EXIT
go build -C "$DIR/$NAME" -o "$TMP/$NAME" .
"$TMP/$NAME" $@
