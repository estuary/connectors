#!/usr/bin/env python3
"""PreToolUse hook (Bash, Write, Edit): the parts of the provider-API gates a live call can't
check for itself.

The rules live in `.claude/shared/provider-api-consent.md`. Two gates run inside the live
call, loaded by the SessionStart hook `gate-env.sh`:

  .claude/bin/sops           API-CONFIG-GATE for every decrypt of a connector's config.yaml
  .claude/hooks/bru-gate.js  API-MUTATE-ONLY-WITH-CONSENT for every request Bruno runs

This hook keeps them loaded and guards the files they read:

  GATE-ENV                   a Bash command that mentions NODE_OPTIONS, sets PATH, clears the
                             environment (`env -i`, `env -u`) or runs sops by absolute path
                             is refused, because it can unload a gate
  API-ROUTE-THROUGH-BRUNO    curl with a write method or a request body, and HTTPie / xh with
                             a write method, are refused
  CONDUCT-PERMISSIONS-FILE   every `permissions.py set` asks the user, even in auto mode, so
                             they approve the exact flags recorded; Write/Edit never touch the
                             permissions file, nor a config.yaml git tracks

The Bash checks match the command text. They parse nothing, so they also refuse a command that
only mentions these words (`grep NODE_OPTIONS …`); rephrase it.

Fails closed: when Python is missing or this hook raises, it exits 2, which blocks every call it
covers.
"""

import json
import os
import re
import subprocess
import sys
from typing import Literal, NoReturn, cast

NEVER_WRITE = (
    "Do not write or edit the permissions file or config.yaml to get past this "
    "(CONDUCT-PERMISSIONS-FILE). Human-in-the-loop: surface it to the user. Autonomous: mark the "
    "affected findings PENDING, note it in the decision ledger, and carry on with what needs no API."
)
MAY_UNLOAD_GATE_PATTERN = re.compile(
    r"NODE_OPTIONS|\bPATH=|\benv\s+(\S+\s+)*(-[a-zA-Z]*[iu]|--ignore-environment|--unset)|(^|[\s;&|(])/\S*/sops\b"
)
SETS_PERMISSIONS_PATTERN = re.compile(r"permissions\.py\s+set\b.*")
# curl with a write method or a request body; HTTPie / xh with a write method.
MUTATING_HTTP_CLIENT_PATTERN = re.compile(
    r"\bcurl\b.*(\s(-X|--request)\s*['\"]?(?i:POST|PUT|PATCH|DELETE)\b"
    r"|\s(-d|-F|-T|--data\S*|--form\S*|--upload-file|--json)(\s|=|$))"
    r"|(^|[\s;&|(])(https?|xhs?)\s(.*\s)?(?i:POST|PUT|PATCH|DELETE)(\s|$)",
    re.S,
)


def decide(decision: Literal["deny", "ask"], reason: str) -> NoReturn:
    print(
        json.dumps(
            {
                "hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                    "permissionDecision": decision,
                    "permissionDecisionReason": reason,
                }
            }
        )
    )
    sys.exit(0)


def is_tracked(path: str) -> bool:
    return (
        subprocess.run(
            [
                "git",
                "-C",
                os.path.dirname(path),
                "ls-files",
                "--error-unmatch",
                os.path.basename(path),
            ],
            capture_output=True,
            text=True,
        ).returncode
        == 0
    )


def check_file_write(path: str) -> None:
    """Write/Edit: the permissions file and a tracked config.yaml belong to the user."""
    if os.path.basename(
        os.path.dirname(path)
    ) == "claude-permissions" and path.endswith(".json"):
        decide(
            "deny",
            f"CONDUCT-PERMISSIONS-FILE: the permissions file changes only through `permissions.py set` / `revoke`. {NEVER_WRITE}",
        )
    connector = os.path.dirname(path)
    if (
        os.path.basename(path) == "config.yaml"
        and any(
            os.path.isfile(os.path.join(connector, f))
            for f in ("pyproject.toml", "go.mod")
        )
        and is_tracked(path)
    ):
        decide(
            "deny",
            (
                "API-CONFIG-GATE: this config.yaml holds the connector's credentials and git tracks it, so only the "
                f"user edits it (with `sops`). {NEVER_WRITE}"
            ),
        )


def check_command(command: str) -> None:
    if MAY_UNLOAD_GATE_PATTERN.search(command):
        decide(
            "deny",
            (
                "GATE-ENV: this command may unload the permission gates (it mentions NODE_OPTIONS, sets PATH, "
                "clears the environment or runs sops by absolute path). Run `bru`, `flowctl` and `sops` with the "
                "shell's own environment. If the command only mentions these words, rephrase it."
            ),
        )
    if MUTATING_HTTP_CLIENT_PATTERN.search(command):
        decide(
            "deny",
            (
                "API-ROUTE-THROUGH-BRUNO: a session never sends a provider a request that changes data "
                "with curl or HTTPie. Author the request under the connector's bruno/Seeding/ folder instead."
            ),
        )
    if m := SETS_PERMISSIONS_PATTERN.search(command):
        decide(
            "ask",
            (
                f"CONDUCT-PERMISSIONS-FILE: `{m.group(0)}` records the user's answers in the permissions file. "
                "Approve only if these flags match what the user said."
            ),
        )


def str_field(obj: object, key: str) -> str:
    """`obj[key]` when `obj` is a JSON object and the value a string, else ""."""
    value = cast(dict[str, object], obj).get(key) if isinstance(obj, dict) else None
    return value if isinstance(value, str) else ""


def main() -> None:
    payload = cast(object, json.load(sys.stdin))
    tool = str_field(payload, "tool_name")
    tool_input = cast(dict[str, object], payload).get("tool_input")
    cwd = str_field(payload, "cwd") or os.getcwd()
    if tool in ("Write", "Edit"):
        check_file_write(
            os.path.abspath(os.path.join(cwd, str_field(tool_input, "file_path")))
        )
    elif tool == "Bash":
        check_command(str_field(tool_input, "command"))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        # Fail closed: exit 2 blocks the tool call, so a broken hook stops every call instead
        # of silently dropping every gate.
        print(
            f"permission-gate.py failed ({exc!r}), so every Bash, Write and Edit call is blocked until it is fixed.",
            file=sys.stderr,
        )
        sys.exit(2)
