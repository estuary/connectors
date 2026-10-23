#!/usr/bin/env bash
#
# Work out which connector CI lanes a set of changed paths affects.
#
# Reads changed paths on stdin, one per line, and writes five lines to stdout:
#
#   python=true|false        a path in the Python lane changed; it can be true
#                            while python_connectors is [], for a Python
#                            directory missing from the connector list
#   go_rust=true|false       the Go & Rust connector workflow must run
#   cdk=true|false           estuary-cdk's own tests and checks must run
#   python_connectors=...    the Python connectors to build: `all` when a path
#                            shared by every one of them changed, otherwise a
#                            compact JSON list of connector directories
#   rust_connectors=...      the Rust connectors to build, in the same form
#
# The lane chosen for each path is logged to stderr.
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

python=false
go_rust=false
cdk=false

# Set when a changed path feeds every Python connector. Otherwise only the
# connector directories collected in $python_dirs are affected.
python_all=false
python_dirs=()

# Likewise for the Rust connectors.
rust_all=false
rust_dirs=()
rust_connectors=$(sed -n 's/^- name: //p' "$REPO_ROOT/.github/rust-connectors.yaml")

# Which Rust connectors a path in the Go & Rust lane affects. A Rust
# connector's image is built only from its own directory and base-image, so
# path rules are enough. Anything they don't recognise builds every one.
rust_select() {
    local path=$1 top=${1%%/*} sub name
    case "$path" in
        # The workflow, its list, and the classifier that decides for it.
        .github/workflows/rust.yaml | .github/rust-connectors.yaml | \
        .github/scripts/classify-changed-paths.sh | .github/scripts/test_classify_changed_paths.py | \
        base-image/*)
            rust_all=true; return ;;
        # Other workflows, CI scripts and connector lists build no Rust connector.
        .github/workflows/* | .github/scripts/* | .github/*-connectors.yaml) return ;;
        # Go's module files, and the Rust crate only source-mongodb builds.
        go.mod | go.sum | bson-transcoder/*) return ;;
        # The shared materialization tests run for every materialize-* connector.
        tests/materialize/*)
            for name in $rust_connectors; do
                case "$name" in materialize-*) rust_dirs+=("$name") ;; esac
            done
            return ;;
    esac

    if [ "$top" != "$path" ] && grep -qxF "$top" <<< "$rust_connectors"; then
        rust_dirs+=("$top"); return
    fi

    # A connector's own test fixtures, or shared test tooling.
    case "$path" in
        tests/*)
            sub=${path#tests/}; sub=${sub%%/*}
            if grep -qxF "$sub" <<< "$rust_connectors"; then
                rust_dirs+=("$sub")
            else
                rust_all=true
            fi
            return ;;
    esac

    # Go code, which no Rust connector builds from: a .go file, or anything in
    # a top-level directory holding Go code, such as a Go connector.
    if [[ "$path" == *.go ]] ||
       { [ "$top" != "$path" ] && [ -n "$(find "$REPO_ROOT/$top" -name '*.go' -print -quit 2>/dev/null)" ]; }; then
        return
    fi

    rust_all=true
}

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
        # connectors-auto-rerun.yaml only re-runs the connector workflows' failed jobs.
        .github/workflows/python.yaml) lane="python" ;;
        .github/workflows/go.yaml) lane="go_rust" ;;
        .github/workflows/rust.yaml) lane="go_rust" ;;
        .github/workflows/connectors-auto-rerun.yaml) lane="none" ;;
        # The shared ruff config builds no connector, since connector lint only
        # reports, but estuary-cdk's lint gates on it.
        ruff.toml) lane="none"; cdk=true ;;

        # The Python connector matrix, which python.yaml reads as data.
        .github/python-connectors.yaml) lane="python" ;;

        # The uv workspace resolves every member from these two files, so a
        # change to either reaches every Python connector. They are named here
        # because the directory heuristic below only looks at a path's first
        # segment, and theirs is the file itself.
        uv.lock | pyproject.toml) lane="python" ;;

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
                # workflow and its connector list, and the workspace manifest
                # and lock, feed every Python connector.
                estuary-cdk/* | .github/workflows/python.yaml | .github/python-connectors.yaml | uv.lock | pyproject.toml)
                    python_all=true ;;
                *) python_dirs+=("${path%%/*}") ;;
            esac
            ;;
        go_rust)
            go_rust=true
            rust_select "$path"
            ;;
        both)
            # Anything shared by both lanes is shared by every connector.
            python=true
            python_all=true
            go_rust=true
            rust_all=true
            ;;
    esac

    echo "  ${lane}: ${path}" >&2
done <<< "$input"

# A change that rebuilds every Python connector also runs estuary-cdk's checks.
# That covers the CDK itself, the workspace manifest and lock, and python.yaml,
# and the few other paths it sweeps in cost little next to the full rebuild.
if [ "$python_all" = true ]; then
    cdk=true
fi

echo "python=${python}"
echo "go_rust=${go_rust}"
echo "cdk=${cdk}"
if [ "$python_all" = true ]; then
    echo "python_connectors=all"
else
    # `${arr[@]+...}` because bash before 4.4 treats an empty array as unset
    # under `set -u`.
    jq -cn '$ARGS.positional | unique' --args ${python_dirs[@]+"${python_dirs[@]}"} |
        sed 's/^/python_connectors=/'
fi
if [ "$rust_all" = true ]; then
    echo "rust_connectors=all"
else
    jq -cn '$ARGS.positional | unique' --args ${rust_dirs[@]+"${rust_dirs[@]}"} |
        sed 's/^/rust_connectors=/'
fi
