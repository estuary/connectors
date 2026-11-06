#!/bin/bash
# Prints the golang image a connector's Dockerfile builds in and the Go build
# tags it compiles with ("default" or "nozstd"), as "<image> <tags>". Prints
# nothing for a Dockerfile that doesn't build in a golang image.
set -euo pipefail

image=$(sed -nE 's/^FROM( --platform=[^ ]+)? (golang:[^ ]+) AS builder.*/\2/p' "$1" | head -n 1)
[ -n "$image" ] || exit 0

tags=default
if grep -q -- '-tags nozstd' "$1"; then tags=nozstd; fi
echo "$image $tags"
