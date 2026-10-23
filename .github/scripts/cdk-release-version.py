#!/usr/bin/env python3
"""Print the PEP 440 version for an estuary-cdk distribution built from HEAD.

Given a tag, that tag's version. Otherwise `<manifest>.dev<commits-since-tag>`,
where estuary-cdk/pyproject.toml's version names the *next* cut:

    manifest 0.4.0, 19 commits since v0.3.0  ->  0.4.0.dev19
    cdk-release-version.py v0.3.0            ->  0.3.0

A tag takes its version from the tag rather than the manifest, because at the
tagged commit the manifest has not been raised yet and the check below would
reject the release it is naming.

`vX.Y.Z` is shared with this repository's Go module version; see
estuary-cdk/README.md. The manifest must stay ahead of the nearest tag, so that
forgetting to raise it after a cut fails here rather than publishing a `.devN`
that sorts below a release already on PyPI.

Needs tags and history, so the calling job checks out with `fetch-depth: 0`.
"""

from __future__ import annotations

import re
import subprocess
import sys
import tomllib
from pathlib import Path

MANIFEST = Path(__file__).resolve().parents[2] / "estuary-cdk" / "pyproject.toml"


def die(message: str) -> None:
    sys.exit(f"error: {message}")


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

    # `--long` gives `<tag>-<offset>-g<sha>` even at the tag itself, so there is
    # one shape to parse rather than two.
    described = subprocess.run(
        ["git", "describe", "--tags", "--match", "v*", "--long"],
        capture_output=True,
        text=True,
    )
    if described.returncode != 0:
        die(f"no v* tag reachable from HEAD; seed one (e.g. 'git tag v{manifest}')")

    tag, offset, _sha = described.stdout.strip().rsplit("-", 2)
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
