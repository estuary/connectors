#!/usr/bin/env bash
#
# Scan connector images with Trivy, under the policy in trivy.yaml.
#
#   .github/scripts/trivy-scan.sh <connector> <image> [<connector> <image>]...
#
# <connector> is the connector's directory, and decides which exceptions
# apply: the root .trivyignore.yaml, for what every image shares, plus
# <connector>/.trivyignore.yaml if there is one. No other connector's file is
# read, so an exception never needs path scoping to stay out of other images.
# A variant is scanned under its base connector's directory, since it is that
# image with different environment variables.
#
# Trivy takes a single ignore file, so the two are merged into a temporary
# one. Both are checked first: each entry needs an `id`, a `statement` and an
# `expired_at` date.
#
# Every image is scanned before the script fails, so one run reports all of
# them. Under GitHub Actions the images that failed, whether on a finding or
# because the scan itself failed, are written to $GITHUB_OUTPUT as `failed`,
# and their connectors as `failed_connectors`, each space-separated.
#
# Trivy reads TRIVY_* environment variables as flags, which take precedence
# over trivy.yaml. So that CI and local runs fail on the same things, only
# TRIVY_IMAGE_SRC and TRIVY_CACHE_DIR, which say where the image and the
# database are rather than what fails, may be set.
#
# For example, against an image built by ./build-local.sh:
#
#   .github/scripts/trivy-scan.sh source-postgres ghcr.io/estuary/source-postgres:local
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

if [ "$#" -eq 0 ] || [ $(($# % 2)) -ne 0 ]; then
    echo "usage: $0 <connector> <image> [<connector> <image>]..." >&2
    exit 2
fi

while IFS= read -r var; do
    case "$var" in
        TRIVY_IMAGE_SRC | TRIVY_CACHE_DIR) ;;
        TRIVY_*)
            echo "${var} is set, and would override trivy.yaml; unset it" >&2
            exit 2
            ;;
    esac
done < <(compgen -e)

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# Print a line for each problem with an ignore file, and nothing if it's fine.
# A file of comments alone parses as null, which is fine. Only
# `vulnerabilities` is accepted, as trivy.yaml runs only the vuln scanner.
check_ignorefile() {
    yq -o=json '.' "$1" | jq -r --arg file "${1#"$ROOT"/}" '
        def problem(msg): "\($file): \(msg)";
        if . == null then empty
        elif type != "object" then problem("expected a mapping with a vulnerabilities list")
        else
            ((keys - ["vulnerabilities"])[] | problem("unexpected key \(.); only vulnerabilities are scanned")),
            (.vulnerabilities // [] |
                if type != "array" then problem("vulnerabilities must be a list")
                else
                    to_entries[] | .key as $i | .value |
                    if type != "object" then problem("entry \($i + 1) is not a mapping")
                    else
                        (.id // "entry \($i + 1)") as $name |
                        (("id", "statement", "expired_at") as $key |
                            select((.[$key] // "" | tostring) == "") |
                            problem("\($name) has no \($key)")),
                        (select(.expired_at != null) |
                            select(.expired_at | tostring | test("^[0-9]{4}-[0-9]{2}-[0-9]{2}$") | not) |
                            problem("\($name) expired_at \(.expired_at) is not a YYYY-MM-DD date"))
                    end
                end)
        end'
}

# Write the exceptions that apply to $1's image to $2.
merge_ignorefiles() {
    local connector=$1 out=$2 files=("$ROOT/.trivyignore.yaml") problems
    if [ -f "$ROOT/$connector/.trivyignore.yaml" ]; then
        files+=("$ROOT/$connector/.trivyignore.yaml")
    fi
    for file in "${files[@]}"; do
        problems=$(check_ignorefile "$file") || problems="${file#"$ROOT"/}: could not be read"
        if [ -n "$problems" ]; then
            echo "$problems" >&2
            return 1
        fi
    done
    yq eval-all '. as $f ireduce ([]; . + ($f.vulnerabilities // [])) | {"vulnerabilities": .}' \
        "${files[@]}" > "$out"
}

failed=()
failed_connectors=()
while [ "$#" -gt 0 ]; do
    connector=$1 image=$2
    shift 2
    echo "Scanning ${image} with the exceptions for ${connector}"

    ignorefile="${work}/${connector}.trivyignore.yaml"
    if ! [[ "$connector" =~ ^[a-z0-9][a-z0-9-]*$ ]] || [ ! -d "$ROOT/$connector" ]; then
        echo "${connector} is not a connector directory" >&2
    elif ! merge_ignorefiles "$connector" "$ignorefile"; then
        :
    elif trivy image --config "$ROOT/trivy.yaml" --ignorefile "$ignorefile" "$image"; then
        continue
    fi

    failed+=("$image")
    # `[*]-` because bash before 4.4 treats an empty array as unset under
    # `set -u`.
    case " ${failed_connectors[*]-} " in
        *" ${connector} "*) ;;
        *) failed_connectors+=("$connector") ;;
    esac
done

if [ -n "${GITHUB_OUTPUT:-}" ]; then
    echo "failed=${failed[*]-}" >> "$GITHUB_OUTPUT"
    echo "failed_connectors=${failed_connectors[*]-}" >> "$GITHUB_OUTPUT"
fi

if [ "${#failed[@]}" -gt 0 ]; then
    echo
    echo "Failed: a HIGH or CRITICAL finding with a fixed release, or a scan that could not run:"
    for image in "${failed[@]}"; do
        if [ "${GITHUB_ACTIONS:-}" = true ]; then
            echo "::error title=Trivy::${image}"
        else
            echo "  ${image}"
        fi
    done
    exit 1
fi
