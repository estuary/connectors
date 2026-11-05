#!/usr/bin/env python3
"""Print the PEP 440 version for an estuary-cdk distribution.

Given a tag, that tag's version. Otherwise `<manifest>.dev<commits-since-tag>`,
where estuary-cdk/pyproject.toml's version names the *next* cut:

    manifest 0.4.0, 19 commits since v0.3.0  ->  0.4.0.dev19
    cdk-release-version.py v0.3.0            ->  0.3.0

The count runs to the last commit that changed WHEEL_PATHS, not to HEAD, and
along main's first-parent line, so that it rises with every commit. Two commits
that build the same wheel therefore get the same version. The publishing job
depends on that; see .github/workflows/python.yaml.

The manifest is read from the working tree, not from that commit. The two agree:
pyproject.toml is one of WHEEL_PATHS, so a commit that raised the version is
itself that last change.

A tag takes its version from the tag rather than the manifest, because at the
tagged commit the manifest has not been raised yet and the check below would
reject the release it is naming.

`vX.Y.Z` is shared with this repository's Go module version; see
estuary-cdk/README.md. The manifest must stay ahead of the nearest tag reachable
from HEAD, so that forgetting to raise it after a cut fails here rather than
publishing a `.devN` that sorts below a release already on PyPI. That tag, not
the one the count runs from: a cut lands on whatever commit is main's tip, which
is usually later than the wheel's last change and so invisible from it.

Needs tags and history, so the calling job checks out with `fetch-depth: 0`.
"""

from __future__ import annotations

import re
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "estuary-cdk" / "pyproject.toml"

# Everything that goes into the wheel: the package [tool.hatch.build.targets.wheel]
# names, and the manifest supplying its metadata. Update this if that table does.
WHEEL_PATHS = ("estuary-cdk/estuary_cdk/", "estuary-cdk/pyproject.toml")


def die(message: str) -> None:
    sys.exit(f"error: {message}")


def git(*args: str) -> subprocess.CompletedProcess[str]:
    # `-C` because WHEEL_PATHS are repository-relative, not cwd-relative.
    return subprocess.run(
        ["git", "-C", str(ROOT), *args], capture_output=True, text=True
    )


def parse(version: str, what: str) -> tuple[int, ...]:
    if not re.fullmatch(r"\d+(?:\.\d+)*", version):
        die(f"{what} {version!r} is not a plain release version")
    return tuple(int(part) for part in version.split("."))


def main(argv: list[str]) -> None:
    with MANIFEST.open("rb") as f:
        manifest = tomllib.load(f)["project"]["version"]
    declared = parse(manifest, "manifest version")

    if argv:
        tag = argv[0]
        if not tag.startswith("v"):
            die(f"tag {tag!r} does not look like vX.Y.Z")
        parse(tag[1:], f"tag {tag}")
        print(tag[1:])
        return

    # `--first-parent`, or a pull request landed as a merge commit resolves to
    # the commit on its branch: the merge is TREESAME to that side for these
    # paths, so plain history simplification walks past it. The offset would
    # then count from the branch's fork point, which neither rises with main nor
    # is unique to a commit.
    found = git(
        "log", "-1", "--first-parent", "--format=%H %h %s", "HEAD", "--", *WHEEL_PATHS
    )
    if found.returncode != 0 or not found.stdout.strip():
        die(f"no commit reachable from HEAD touches {' '.join(WHEEL_PATHS)}")

    commit, summary = found.stdout.strip().split(" ", 1)

    # %s is history text whoever wrote the commit controls, and the runner reads
    # a step's log for workflow commands. Git keeps a `\r` in a subject, and
    # `text=True` turns it into a newline; collapsing whitespace is the fix,
    # because a command has to start a line. The legacy `##[` prefix is defused
    # as well.
    summary = " ".join(summary.split()).replace("##[", "#[")

    # stderr, so that the caller's `$(...)` still captures the version alone.
    print(f"the wheel last changed in {summary}", file=sys.stderr)

    # `--long` gives `<tag>-<offset>-g<sha>` even at the tag itself, so there is
    # one shape to parse rather than two.
    described = git("describe", "--tags", "--match", "v*", "--long", commit)
    if described.returncode != 0:
        die(f"no v* tag reachable from {commit}; seed one (e.g. 'git tag v{manifest}')")

    offset = described.stdout.strip().rsplit("-", 2)[1]

    # The offset counts from the wheel commit, but the manifest is checked
    # against HEAD. A release cut after that commit is not reachable from it, so
    # describing it alone would not see the very tag whose forgotten manifest
    # bump this is here to catch -- and would hand back a `.devN` below it.
    at_head = git("describe", "--tags", "--match", "v*", "--long", "HEAD")
    if at_head.returncode != 0:
        die(f"no v* tag reachable from HEAD; seed one (e.g. 'git tag v{manifest}')")

    tag, _offset, _sha = at_head.stdout.strip().rsplit("-", 2)
    nearest = parse(tag[1:], f"tag {tag}")

    if declared <= nearest:
        die(
            f"estuary-cdk/pyproject.toml declares {manifest}, which is not ahead of "
            f"the nearest tag {tag}. Cutting a release means tagging, then raising "
            f"the manifest to the next version."
        )

    print(f"{manifest}.dev{int(offset)}")


if __name__ == "__main__":
    main(sys.argv[1:])
