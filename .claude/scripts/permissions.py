#!/usr/bin/env python3
"""Manage a run's permissions file, `.claude/permissions/<connector>.json` (gitignored).

The file records the user's questionnaire answers for one connector, for one run, and is what
`.claude/hooks/permission-gate.py` checks before any live provider call. Only the orchestrating
skill runs this, and only from the user's live answers.

  permissions.py grant  <connector> --mode human-in-the-loop|autonomous --seeding assistant|user|none
  permissions.py stamp  <connector>    # after the credential checkpoint: binds the permission to
                                       # the committed config.yaml (refuses if it is dirty/untracked)
  permissions.py revoke <connector>    # at hand-off
"""
import argparse
import hashlib
import json
import os
import subprocess
import sys

ROOT = os.environ.get("CLAUDE_PROJECT_DIR") or os.getcwd()


def path(connector):
    return os.path.join(ROOT, ".claude", "permissions", connector + ".json")


def load(connector):
    p = path(connector)
    if not os.path.isfile(p):
        sys.exit(f"no permissions file for {connector}; run `grant` first")
    with open(p) as fh:
        return json.load(fh)


def save(connector, data):
    p = path(connector)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w") as fh:
        json.dump(data, fh, indent=2)
        fh.write("\n")
    print(f"{os.path.relpath(p, ROOT)}: {json.dumps(data)}")


def grant(args):
    save(args.connector, {"mode": args.mode, "seeding": args.seeding})


def stamp(args):
    data = load(args.connector)
    cdir = os.path.join(ROOT, args.connector)
    cfg = os.path.join(cdir, "config.yaml")
    if not os.path.isfile(cfg):
        sys.exit(f"{args.connector}/config.yaml does not exist")
    tracked = subprocess.run(["git", "-C", cdir, "ls-files", "--error-unmatch", "config.yaml"],
                             capture_output=True).returncode == 0
    status = subprocess.run(["git", "-C", cdir, "status", "--porcelain", "--", "config.yaml"],
                            capture_output=True, text=True).stdout.strip()
    if not tracked or status:
        sys.exit(f"{args.connector}/config.yaml is {'untracked' if not tracked else 'modified'}; "
                 "the user must commit it before permission can be bound to it")
    with open(cfg, "rb") as fh:
        data["config_sha256"] = hashlib.sha256(fh.read()).hexdigest()
    save(args.connector, data)


def revoke(args):
    p = path(args.connector)
    if os.path.isfile(p):
        os.remove(p)
    print(f"removed {os.path.relpath(p, ROOT)}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("grant")
    g.add_argument("connector")
    g.add_argument("--mode", required=True, choices=["human-in-the-loop", "autonomous"])
    g.add_argument("--seeding", required=True, choices=["assistant", "user", "none"])
    g.set_defaults(fn=grant)
    s = sub.add_parser("stamp")
    s.add_argument("connector")
    s.set_defaults(fn=stamp)
    r = sub.add_parser("revoke")
    r.add_argument("connector")
    r.set_defaults(fn=revoke)
    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
