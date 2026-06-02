#!/bin/sh
set -e
TMP="$(mktemp -d)"
DIR="$(dirname $0)"
NAME="$(basename $0 .sh)"
trap "rm -rf '$TMP'" EXIT
go build -C "$DIR/$NAME" -o "$TMP/$NAME" .
# "$@" must be quoted: flag values like --check-sql and --approver contain spaces,
# and unquoted $@ would word-split them (truncating a predicate at its first space).
"$TMP/$NAME" "$@"
