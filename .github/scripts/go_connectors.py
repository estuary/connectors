#!/usr/bin/env python3
"""
Work out which Go connectors a set of changed paths affects.

Reads changed paths on stdin, one per line, and prints `all` or a compact JSON
list of Go connector directories. classify-changed-paths.sh calls it with the
paths it routed to the Go & Rust lane.

A shared Go package affects exactly the connectors that import it, directly or
not, so the in-repo import graph decides. It comes from

    go list -e -f '{{.ImportPath}}|{{join .Imports ","}}|{{join .TestImports ","}}|{{join .XTestImports ","}}' ./...

which parses imports from source, so it needs no module downloads. When the
graph is missing or unreadable, every connector is affected.

The reason for each path is logged to stderr.
"""

import argparse
import json
import sys
from pathlib import Path

MODULE = "github.com/estuary/connectors/"

# Directories outside the Go import graph that a Go connector's Dockerfile
# copies into its image. test_connector_lists.py checks every Dockerfile against
# this, so a new one can't go unnoticed.
NON_GO_INPUTS = {
    "bson-transcoder": ["source-mongodb"],
}

# Paths whose change affects every Go connector: what go.yaml is and reads,
# the classifier that decides for it, and the inputs of every image.
ALL_PATHS = {
    ".github/workflows/go.yaml",
    ".github/go-connectors.yaml",
    ".github/scripts/classify-changed-paths.sh",
    ".github/scripts/test_classify_changed_paths.py",
    ".github/scripts/go_connectors.py",
    ".github/scripts/test_go_connectors.py",
    "go.mod",
    "go.sum",
}
ALL_PREFIXES = ("base-image/",)

# Other CI files build no Go connector.
NONE_PREFIXES = (".github/workflows/", ".github/scripts/")


def read_graph(path: str) -> dict[str, set[str]]:
    """Map each in-repo package directory to the in-repo directories it imports."""
    graph: dict[str, set[str]] = {}
    with open(path) as f:
        for line in f:
            fields = line.rstrip("\n").split("|")
            if len(fields) != 4 or not fields[0].startswith(MODULE):
                raise ValueError(f"unexpected go list line: {line!r}")
            imports = {
                i[len(MODULE):]
                for field in fields[1:]
                for i in field.split(",")
                if i.startswith(MODULE)
            }
            graph[fields[0][len(MODULE):]] = imports
    if not graph:
        raise ValueError("no packages listed")
    return graph


def read_list(path: str) -> list[str]:
    """Read connector names from a list of `- name: <connector>` entries."""
    names = []
    with open(path) as f:
        for line in f:
            if line.startswith("- name: "):
                names.append(line[len("- name: "):].strip())
    if not names:
        raise ValueError(f"no connectors read from {path}")
    return names


def closures(graph: dict[str, set[str]], connectors: list[str]) -> dict[str, set[str]]:
    """Map each connector to every in-repo package its own packages reach."""
    out = {}
    for c in connectors:
        roots = [p for p in graph if p == c or p.startswith(c + "/")]
        seen, stack = set(roots), list(roots)
        while stack:
            for dep in graph.get(stack.pop(), ()):
                if dep not in seen:
                    seen.add(dep)
                    stack.append(dep)
        out[c] = seen
    return out


def package_of(path: str, graph: dict[str, set[str]]) -> str | None:
    """The nearest directory above path that is a Go package, if any.

    Files a package embeds or reads in its tests, such as testdata and
    snapshots, live under its directory, so they count as that package.
    """
    parts = path.split("/")[:-1]
    while parts:
        candidate = "/".join(parts)
        if candidate in graph:
            return candidate
        parts.pop()
    return None


def select(
    paths: list[str],
    graph: dict[str, set[str]],
    connectors: list[str],
    rust_connectors: list[str],
) -> str | list[str]:
    reach = closures(graph, connectors)
    selected: set[str] = set()

    def log(reason: str, path: str) -> None:
        print(f"  {reason}: {path}", file=sys.stderr)

    for path in paths:
        top = path.split("/", 1)[0]
        if path in ALL_PATHS or path.startswith(ALL_PREFIXES):
            log("all", path)
            return "all"
        if path.startswith(NONE_PREFIXES) or (
            path.startswith(".github/") and path.endswith("-connectors.yaml")
        ):
            log("none", path)
            continue
        if path.startswith("tests/materialize/"):
            hits = [c for c in connectors if c.startswith("materialize-")]
            log("materializations", path)
            selected.update(hits)
            continue
        if path.startswith("tests/"):
            sub = path.split("/")[1] if path.count("/") >= 2 else ""
            if sub in connectors:
                log(sub, path)
                selected.add(sub)
                continue
            log("all", path)
            return "all"
        if top in rust_connectors and top != path:
            log("none (a Rust connector)", path)
            continue
        if top in NON_GO_INPUTS and top != path:
            log(",".join(NON_GO_INPUTS[top]), path)
            selected.update(NON_GO_INPUTS[top])
            continue

        hits = set()
        if top in connectors and top != path:
            hits.add(top)
        package = package_of(path, graph)
        if package is not None:
            hits.update(c for c in connectors if package in reach[c])
        if hits:
            log(",".join(sorted(hits)), path)
            selected.update(hits)
        elif package is not None:
            log("none (imported by no Go connector)", path)
        else:
            # When in doubt, build.
            log("all", path)
            return "all"

    return sorted(selected)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--graph", required=True, help="go list output, as above")
    parser.add_argument("--connectors", required=True, help="the Go connector list")
    parser.add_argument("--rust-connectors", required=True, help="the Rust connector list")
    args = parser.parse_args()

    paths = [line.strip() for line in sys.stdin if line.strip()]
    try:
        graph = read_graph(args.graph)
        connectors = read_list(args.connectors)
        rust_connectors = read_list(args.rust_connectors)
    except (OSError, ValueError) as e:
        print(f"  all: no usable import graph ({e})", file=sys.stderr)
        print("all")
        return

    result = select(paths, graph, connectors, rust_connectors)
    print(result if result == "all" else json.dumps(result, separators=(",", ":")))


if __name__ == "__main__":
    main()
