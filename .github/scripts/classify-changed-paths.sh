#!/usr/bin/env bash
#
# Work out which connector CI lanes a set of changed paths affects.
#
# Reads changed paths on stdin, one per line, and writes three lines to stdout:
#
#   python=true|false        a path in the Python lane changed; it can be true
#                            while python_connectors is [], for a Python
#                            directory missing from the connector list
#   go_rust=true|false       the Go & Rust connector workflow must run
#   python_connectors=...    the Python connectors to build: `all` when a path
#                            shared by every one of them changed, otherwise a
#                            compact JSON list of connector directories
#
# The lane chosen for each path is logged to stderr.
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

python=false
go_rust=false

# Set when a changed path feeds every Python connector. Otherwise only the
# connector directories collected in $python_dirs are affected.
python_all=false
python_dirs=()

# Read stdin up front so it can be scanned before classifying. Deleting a
# connector takes its pyproject.toml off disk, so the changed paths are the only
# evidence it was a Python one. `|| true` because grep exits non-zero when
# nothing matches, which `set -e` would treat as fatal.
input=$(cat)
changed_pyproject_dirs=$(
    grep -E "^[^/]+/pyproject\.toml$" <<< "$input" | cut -d/ -f1 | sort -u || true
)

# `|| [ -n "$path" ]` so a final line with no trailing newline is still read.
# Without it that path is silently dropped from the verdict, which could skip a
# build for a change it didn't notice.
while IFS= read -r path || [ -n "$path" ]; do
    [ -n "$path" ] || continue

    lane=""
    case "$path" in
        # Documentation and agent tooling build nothing.
        *.md | docs/* | .claude/*) lane="none" ;;

        # Each workflow is triggered by edits to itself so a change to one
        # verifies itself rather than waiting on an unrelated connector edit.
        # estuary-cdk.yaml drives its own separate workflow, and
        # python-auto-rerun.yaml only re-runs python.yaml's failed jobs.
        .github/workflows/python.yaml) lane="python" ;;
        .github/workflows/ci.yaml) lane="go_rust" ;;
        .github/workflows/estuary-cdk.yaml) lane="none" ;;
        .github/workflows/python-auto-rerun.yaml) lane="none" ;;
        # The shared ruff config only feeds report-only lint steps, so it
        # builds nothing. estuary-cdk.yaml lints the CDK when it changes.
        ruff.toml) lane="none" ;;

        # The Python connector matrix, which python.yaml reads as data.
        .github/python-connectors.yaml) lane="python" ;;

        # Shared build plumbing that both workflows invoke. Both workflows also
        # build images with the repo root as the Docker context, so the root
        # .dockerignore shapes every image, and the deploy action layers
        # connector-variant.Dockerfile onto every image it pushes.
        .github/actions/* | fetch-flow.sh | .dockerignore | connector-variant.Dockerfile) lane="both" ;;
    esac

    if [ -z "$lane" ]; then
        # Python connectors and estuary-cdk all carry a top-level pyproject.toml.
        # The check is one level deep on purpose. materialize-iceberg/python has
        # a pyproject.toml of its own, and a recursive check that would classify a
        # Go connector as Python and stop building it.
        #
        # "$top" != "$path" keeps top-level files such as go.mod out of this
        # branch. $changed_pyproject_dirs covers a connector deleted by this
        # change whose manifest is gone from disk but still listed among the
        # changed paths.
        top=${path%%/*}
        if [ "$top" != "$path" ] &&
           # Is there a pyproject.toml in the connector's root?
           { [ -f "$REPO_ROOT/$top/pyproject.toml" ] ||
             # Or does $top appear as a complete line, matched literally, in the set of
             # directories whose pyproject.toml this change touched? This catches the
             # case where a Python connector is deleted wholesale.
             grep -qxF "$top" <<< "$changed_pyproject_dirs"; }; then
            lane="python"
        else
            # Go and Rust connectors, the shared Go packages, go.mod, base-image,
            # tests/.
            lane="go_rust"
        fi
    fi

    case "$lane" in
        python)
            python=true
            case "$path" in
                # The CDK (and the Dockerfiles connectors symlink to), the
                # workflow and its connector list feed every Python connector.
                estuary-cdk/* | .github/workflows/python.yaml | .github/python-connectors.yaml)
                    python_all=true ;;
                *) python_dirs+=("${path%%/*}") ;;
            esac
            ;;
        go_rust) go_rust=true ;;
        both)
            # Anything shared by both lanes is shared by every Python connector.
            python=true
            python_all=true
            go_rust=true
            ;;
    esac

    echo "  ${lane}: ${path}" >&2
done <<< "$input"

echo "python=${python}"
echo "go_rust=${go_rust}"
if [ "$python_all" = true ]; then
    echo "python_connectors=all"
else
    # `${arr[@]+...}` because bash before 4.4 treats an empty array as unset
    # under `set -u`.
    jq -cn '$ARGS.positional | unique' --args ${python_dirs[@]+"${python_dirs[@]}"} |
        sed 's/^/python_connectors=/'
fi
