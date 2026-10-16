#!/usr/bin/env python3
"""PreToolUse hook (Bash): mechanical enforcement of the provider-API gates.

The rules live in `.claude/shared/provider-api-consent.md`; this hook enforces the ones a
shell command reveals, so no skill has to restate them and no session has to be trusted:

  API-CONFIG-GATE          no live run (`bru run`, `flowctl raw preview-next|discover|capture`,
                           `pytest` inside a connector) while that connector's config.yaml has
                           uncommitted changes, or no longer matches the hash the user gave
                           permission for
  API-MUTATE-ONLY-WITH-CONSENT
                           a `bru run` covering any non-GET/HEAD request needs this run's
                           permissions file to say `seeding: assistant`
  API-ROUTE-THROUGH-BRUNO  mutating `curl` / HTTPie are refused outright

Permissions file: `.claude/permissions/<connector>.json` (gitignored), managed only through
`.claude/scripts/permissions.py` (grant / stamp / revoke) by the orchestrating skill.
Shape: {"mode": "human-in-the-loop"|"autonomous", "seeding": "assistant"|"user"|"none",
"config_sha256": "<sha256 of <connector>/config.yaml>" (added by `stamp`)}.

Deny-only by design: the hook emits `deny` (or `ask` in human-in-the-loop mode) and is otherwise
silent. Project hooks run before the workspace-trust dialog, so a hook that could *grant*
would be unsafe to ship in a repo; one that can only refuse is not. Every denial message
says what to do instead — that text is the instruction, delivered at the moment it applies,
so the skills don't have to carry it.
"""
import hashlib
import json
import os
import re
import shlex
import subprocess
import sys

READ_METHODS = {"GET", "HEAD", "OPTIONS"}
MUTATING_VERBS = {"POST", "PUT", "PATCH", "DELETE"}
# `bru run` options that take a value, so the value isn't mistaken for the positional target.
BRU_VALUE_OPTS = {"--env", "--env-var", "--sandbox", "--output", "-o", "--format", "-f",
                  "--reporter-json", "--reporter-junit", "--reporter-html", "--reporter-skip-headers",
                  "--cacert", "--client-cert-config", "--delay", "--tags", "--exclude-tags",
                  "--csv-file-path", "--json-file-path", "--iteration-count"}

NEVER_WRITE = ("Do not write or edit the permissions file or config.yaml to get past this "
               "(CONDUCT-PERMISSIONS-FILE). Human-in-the-loop: surface it to the user. Autonomous: mark the "
               "affected findings PENDING, note it in the decision ledger, and carry on with what needs no API.")


def decide(decision, reason):
    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": decision,
            "permissionDecisionReason": reason,
        }
    }))
    sys.exit(0)


def project_dir():
    return os.environ.get("CLAUDE_PROJECT_DIR") or os.getcwd()


def permissions_path(connector):
    return os.path.join(".claude", "permissions", connector + ".json")


def read_permissions(connector):
    """The permissions file as a dict, None if absent. Unparseable → deny: a permission that
    can't be read is not a permission."""
    path = os.path.join(project_dir(), permissions_path(connector))
    if not os.path.isfile(path):
        return None
    try:
        with open(path) as fh:
            out = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        decide("deny", f"{path} is not valid JSON ({exc}). {NEVER_WRITE}")
    if not isinstance(out, dict) or not all(isinstance(v, str) for v in out.values()):
        decide("deny", f"{path} must be a JSON object of string values. {NEVER_WRITE}")
    return out


def split_segments(command):
    """Split a shell command on && ; || | and newlines, keeping each segment's tokens."""
    segments, current = [], []
    try:
        tokens = shlex.split(command, posix=True)
    except ValueError:
        return [command.split()]
    for tok in tokens:
        if tok in ("&&", ";", "||", "|"):
            segments.append(current)
            current = []
        else:
            current.append(tok)
    segments.append(current)
    return [s for s in segments if s]


def curl_is_mutation(tokens):
    for i, tok in enumerate(tokens):
        if tok in ("-X", "--request") and i + 1 < len(tokens) and tokens[i + 1].upper() in MUTATING_VERBS:
            return True
        if tok.startswith("-X") and tok[2:].upper() in MUTATING_VERBS:
            return True
        if tok in ("-d", "--data", "--data-raw", "--data-binary", "--data-urlencode", "-F", "--form", "-T", "--upload-file"):
            return True
    return False


def httpie_is_mutation(tokens):
    return any(t.upper() in MUTATING_VERBS for t in tokens[1:] if not t.startswith("-"))


def walk_up(start, found):
    """Walk up from `start` to the first directory for which found(dir) is true."""
    p = os.path.abspath(start)
    if os.path.isfile(p):
        p = os.path.dirname(p)
    for _ in range(12):
        if found(p):
            return p
        parent = os.path.dirname(p)
        if parent == p:
            break
        p = parent
    return None


def find_collection_root(start):
    return walk_up(start, lambda p: os.path.isfile(os.path.join(p, "opencollection.yml"))
                   or os.path.isfile(os.path.join(p, "bruno.json")))


def find_connector_dir(start):
    """A directory holding config.yaml next to a pyproject.toml or go.mod."""
    return walk_up(start, lambda p: os.path.isfile(os.path.join(p, "config.yaml")) and (
        os.path.isfile(os.path.join(p, "pyproject.toml")) or os.path.isfile(os.path.join(p, "go.mod"))))


def request_methods(target):
    """Methods of every request file a `bru run <target>` covers."""
    files = []
    if os.path.isfile(target):
        files = [target]
    elif os.path.isdir(target):
        for root, dirs, names in os.walk(target):
            dirs[:] = [d for d in dirs if d != "environments"]
            for n in names:
                if n.endswith((".yml", ".yaml", ".bru")) and n not in ("opencollection.yml", "folder.yml", "collection.bru", "folder.bru"):
                    files.append(os.path.join(root, n))
    methods = {}
    for f in files:
        try:
            text = open(f, errors="replace").read()
        except OSError:
            continue
        m = re.search(r"^\s*method:\s*([A-Za-z]+)", text, re.M)  # OpenCollection YAML
        if not m:
            m = re.search(r"^(get|post|put|patch|delete|head|options)\s*\{", text, re.M | re.I)  # legacy .bru
        if m:
            methods[f] = m.group(1).upper()
    return methods


def config_dirty(connector_dir):
    cfg = os.path.join(connector_dir, "config.yaml")
    if not os.path.isfile(cfg):
        return False, None
    try:
        tracked = subprocess.run(["git", "-C", connector_dir, "ls-files", "--error-unmatch", "config.yaml"],
                                 capture_output=True, text=True).returncode == 0
        status = subprocess.run(["git", "-C", connector_dir, "status", "--porcelain", "--", "config.yaml"],
                                capture_output=True, text=True).stdout.strip()
    except OSError:
        return False, None
    if not tracked:
        return True, "config.yaml is untracked"
    if status:
        return True, f"git status --porcelain reports `{status}`"
    return False, None


def config_hash(connector_dir):
    cfg = os.path.join(connector_dir, "config.yaml")
    if not os.path.isfile(cfg):
        return None
    with open(cfg, "rb") as fh:
        return hashlib.sha256(fh.read()).hexdigest()


def check_credentials(connector_dir):
    """API-CONFIG-GATE for any live run against `connector_dir`. Returns (consent, bound, current)
    so a caller can go on to check mutation consent."""
    connector = os.path.basename(connector_dir)
    dirty, why = config_dirty(connector_dir)
    if dirty:
        decide("deny",
               f"API-CONFIG-GATE: {connector}/config.yaml has uncommitted changes ({why}). The credentials may "
               f"now point at a different account, so no live call runs until the user commits the file. {NEVER_WRITE}")
    consent = read_permissions(connector)
    current = config_hash(connector_dir)
    bound = (consent or {}).get("config_sha256")
    if consent and bound and current and bound != current:
        decide("ask" if consent.get("mode") == "human-in-the-loop" else "deny",
               f"API-CONFIG-GATE: {connector}/config.yaml no longer matches the credentials the user gave permission "
               f"for (permissions file {bound[:12]}…, config.yaml {current[:12]}…). No live call runs until the user "
               f"re-confirms and the orchestrator re-runs `permissions.py stamp {connector}`. {NEVER_WRITE}")
    return consent, bound, current


def handle_bru_run(tokens, cwd):
    args, skip = [], False
    for t in tokens[2:]:
        if skip:
            skip = False
        elif t in BRU_VALUE_OPTS:
            skip = True
        elif not t.startswith("-"):
            args.append(t)
    # `bru run` with no positional runs the whole collection from cwd.
    target = os.path.join(cwd, args[0]) if args and not os.path.isabs(args[0]) else (args[0] if args else cwd)
    root = find_collection_root(target) or find_collection_root(cwd)
    if root is None:
        return  # not a Bruno collection we recognise; leave it to the normal permission flow
    connector_dir = os.path.dirname(root)
    connector = os.path.basename(connector_dir)

    consent, bound, current = check_credentials(connector_dir)

    methods = request_methods(target if os.path.exists(target) else root)
    mutating = {f: m for f, m in methods.items() if m not in READ_METHODS}
    if not mutating:
        return
    if consent and consent.get("seeding") == "assistant" and bound and bound == current:
        return  # explicit `seeding: assistant`, bound to the credentials file actually in place

    names = [f"{os.path.relpath(f, root)} ({m})" for f, m in sorted(mutating.items())]
    listed = ", ".join(names[:5]) + (f", +{len(names) - 5} more" if len(names) > 5 else "")
    if consent is None:
        state = "there is no permissions file for this connector (the questionnaire has not been run)"
    elif consent.get("seeding") != "assistant":
        state = f"the user's recorded answer is `seeding: {consent.get('seeding', '?')}`"
    else:
        state = "`seeding: assistant` is recorded but not yet stamped to a committed config.yaml"
    reason = (f"API-MUTATE-ONLY-WITH-CONSENT: this `bru run` covers mutating requests — {listed} — and {state}. "
              "Human-in-the-loop with `seeding: user`: hand the request to the user and resume when they confirm. "
              f"Otherwise leave the finding PENDING (GATE-SEEDING). {NEVER_WRITE}")
    decide("ask" if consent and consent.get("mode") == "human-in-the-loop" else "deny", reason)


def handle_live_run(tokens, cwd):
    """`flowctl raw preview-next|discover|capture --source <flow.yaml>` and `pytest` inside a connector."""
    target = cwd
    for i, t in enumerate(tokens):
        if t == "--source" and i + 1 < len(tokens):
            target = os.path.join(cwd, tokens[i + 1])
        elif t.startswith("--source="):
            target = os.path.join(cwd, t.split("=", 1)[1])
    connector_dir = find_connector_dir(target)
    if connector_dir is not None:
        check_credentials(connector_dir)


def main():
    try:
        payload = json.load(sys.stdin)
    except json.JSONDecodeError:
        return
    if payload.get("tool_name") != "Bash":
        return
    command = (payload.get("tool_input") or {}).get("command") or ""
    cwd = payload.get("cwd") or os.getcwd()

    for seg in split_segments(command):
        # Track `cd <dir>` so `cd source-x/bruno && bru run …` resolves against the right directory.
        if seg[0] == "cd" and len(seg) > 1:
            cwd = os.path.abspath(os.path.join(cwd, os.path.expanduser(seg[1])))
            continue
        if seg[:2] == ["poetry", "run"]:
            seg = seg[2:]
        if not seg:
            continue
        head = os.path.basename(seg[0])
        if head == "curl" and curl_is_mutation(seg):
            decide("deny", "API-ROUTE-THROUGH-BRUNO: a mutating `curl` against a provider is never run from a session. "
                           "Author the request under the connector's bruno/Seeding/ folder instead.")
        if head in ("http", "https", "xh") and httpie_is_mutation(seg):
            decide("deny", "API-ROUTE-THROUGH-BRUNO: mutating HTTPie call refused; author it under bruno/Seeding/ instead.")
        if head == "bru" and len(seg) > 1 and seg[1] == "run":
            handle_bru_run(seg, cwd)
        if head == "flowctl" and len(seg) > 2 and seg[1] == "raw" and seg[2] in ("preview-next", "discover", "capture"):
            handle_live_run(seg, cwd)
        if head == "pytest":
            handle_live_run(seg, cwd)


if __name__ == "__main__":
    main()
