#!/usr/bin/env python3
"""Manage a run's permissions file, `<git common dir>/claude-permissions/<connector>.json`.

The file lives in the repository's shared `.git` directory so every worktree of
the repo reads the same one. It records the user's questionnaire answers for
one connector, for one run, and is what the permission gates (`.claude/bin/sops`,
`.claude/hooks/bru-gate.js`) check on every live provider call. Only the
orchestrating skill runs this, and only from the user's live answers.

  permissions.py set    <connector> --mode human-in-the-loop|autonomous --seeding assistant|user|none [--stamp]
  permissions.py revoke <connector>    # at hand-off

`set` records the answers and keeps any existing stamp. `--stamp`, after the user confirms the
committed config.yaml at the credential checkpoint, also binds the permission to that file's
`sops.mac` (refused while it is untracked or modified). The mac changes whenever a credential
value does, and survives the JSON conversion flowctl does before it decrypts, so the sops
wrapper can compare it on every caller's decrypt.
"""

import argparse
import json
import os
import re
import subprocess
import sys
from typing import cast

ROOT = os.environ.get("CLAUDE_PROJECT_DIR") or os.getcwd()
# The same pattern as `.claude/bin/sops`; change both together.
MAC = re.compile(r"""["']?mac["']?\s*:\s*["']?(ENC\[[^\]]*\])""")


class Args(argparse.Namespace):
    """The parsed command line; `mode`, `seeding` and `stamp` are set only for `set`."""

    cmd: str = ""
    connector_slug: str = ""
    mode: str = ""
    seeding: str = ""
    stamp: bool = False


def path(connector_slug: str) -> str:
    common = subprocess.run(
        ["git", "-C", ROOT, "rev-parse", "--path-format=absolute", "--git-common-dir"],
        capture_output=True,
        text=True,
    )
    if common.returncode != 0:
        sys.exit(f"{ROOT} is not inside a git repository: {common.stderr.strip()}")
    return os.path.join(
        common.stdout.strip(), "claude-permissions", connector_slug + ".json"
    )


def load(connector_slug: str) -> dict[str, str]:
    """The existing permissions file, or {} when there is none yet."""
    p = path(connector_slug)
    if not os.path.isfile(p):
        return {}
    with open(p) as fh:
        return cast(dict[str, str], json.load(fh))


def save(connector_slug: str, data: dict[str, str]) -> None:
    p = path(connector_slug)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w") as fh:
        json.dump(data, fh, indent=2)
        _ = fh.write("\n")
    print(f"{p}: {json.dumps(data)}")


def set_permissions(args: Args) -> None:
    data = load(args.connector_slug)
    data.update(mode=args.mode, seeding=args.seeding)
    if args.stamp:
        data["config_mac"] = config_mac(args.connector_slug)
    save(args.connector_slug, data)


def config_mac(connector_slug: str) -> str:
    """The committed config.yaml's `sops.mac`; exits while it is untracked, modified or not
    sops-encrypted."""
    cdir = os.path.join(ROOT, connector_slug)
    cfg = os.path.join(cdir, "config.yaml")
    if not os.path.isfile(cfg):
        sys.exit(f"{connector_slug}/config.yaml does not exist")
    ls_files = subprocess.run(
        ["git", "-C", cdir, "ls-files", "--error-unmatch", "config.yaml"],
        capture_output=True,
    )
    tracked = ls_files.returncode == 0
    status = subprocess.run(
        ["git", "-C", cdir, "status", "--porcelain", "--", "config.yaml"],
        capture_output=True,
        text=True,
    ).stdout.strip()
    if not tracked or status:
        sys.exit(
            (
                f"{connector_slug}/config.yaml is {'untracked' if not tracked else 'modified'}; "
                "the user must commit it before permission can be bound to it"
            )
        )
    with open(cfg) as fh:
        mac = MAC.search(fh.read())
    if mac is None:
        sys.exit(
            (
                f"{connector_slug}/config.yaml is not sops-encrypted, so no gate can tell when its "
                "credentials change; the user encrypts it with sops and commits it first"
            )
        )
    return mac.group(1)


def revoke(args: Args) -> None:
    p = path(args.connector_slug)
    if os.path.isfile(p):
        os.remove(p)
    print(f"removed {p}")


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("set")
    _ = s.add_argument("connector_slug", metavar="connector")
    _ = s.add_argument(
        "--mode", required=True, choices=["human-in-the-loop", "autonomous"]
    )
    _ = s.add_argument(
        "--seeding", required=True, choices=["assistant", "user", "none"]
    )
    _ = s.add_argument(
        "--stamp",
        action="store_true",
        help="also bind the permission to the committed config.yaml",
    )

    r = sub.add_parser("revoke")
    _ = r.add_argument("connector_slug", metavar="connector")

    args = ap.parse_args(namespace=Args())
    handlers = {"set": set_permissions, "revoke": revoke}
    handlers[args.cmd](args)


if __name__ == "__main__":
    main()
