#!/usr/bin/env bash
#
# List the connector images the connector workflows publish, one
# `<connector> <image>` line each, for trivy-scan.sh to scan: every connector's
# image and each of its variants', under the version tag that a merge to main
# moves.
#
# Go and Rust connectors take their version and variants from VERSION and
# VARIANTS in their directory, as go.yaml and rust.yaml do. Python connectors
# take them from their entry in python-connectors.yaml, as python.yaml does.
#
# A variant is listed under its base connector, whose exceptions apply to it:
# it is that connector's image with different environment variables.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
REGISTRY=ghcr.io/estuary

# emit <connector> <version> [<variant>...]
emit() {
    local connector=$1 version=$2
    shift 2
    if ! [[ "$version" =~ ^v[0-9]+$ ]]; then
        echo "${connector}: unexpected version '${version}'" >&2
        exit 1
    fi
    for image in "$connector" "$@"; do
        echo "${connector} ${REGISTRY}/${image}:${version}"
    done
}

for lane in go rust; do
    # Assigned rather than iterated over directly, so a failed read fails the
    # script instead of listing nothing.
    names=$(yq '.[].name' "$ROOT/.github/${lane}-connectors.yaml")
    [ -n "$names" ]
    for connector in $names; do
        version=$(tr -d '[:space:]' < "$ROOT/${connector}/VERSION")
        variants=""
        if [ -f "$ROOT/${connector}/VARIANTS" ]; then
            variants=$(cat "$ROOT/${connector}/VARIANTS")
        fi
        # shellcheck disable=SC2086 # VARIANTS is space-separated
        emit "$connector" "$version" $variants
    done
done

entries=$(yq -r '.[] | [.name, .version, .variants // ""] | @tsv' "$ROOT/.github/python-connectors.yaml")
[ -n "$entries" ]
while IFS=$'\t' read -r connector version variants; do
    # shellcheck disable=SC2086 # variants is space-separated, as deploy takes it
    emit "$connector" "$version" $variants
done <<< "$entries"
