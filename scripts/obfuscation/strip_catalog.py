#!/usr/bin/env python3
"""Strip a Flow catalog's connector configs down to customer-agnostic knobs.

Connector endpoint configs and binding resource configs carry
customer-identifying data — hosts, credentials, account/user names, database,
schema and table names, buckets, regions, etc. This tool removes ALL of it and
keeps ONLY keys that appear on a curated allow-list of behavioural knobs that
reveal nothing about the customer (e.g. `hardDelete`, `batchSize`, `format`).

The model is an **allow-list, strip-by-default**: a value survives only if its
own leaf key name is allow-listed. Everything unknown is dropped, so the output
is guaranteed not to expose customer information even as connectors add new,
unclassified config fields. (The trade-off is that new agnostic knobs are
stripped until added to the allow-list — safe, not lossy of anything sensitive.)

The allow-list is read from `config_allowlist.txt` next to this script.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys

ALLOWLIST_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config_allowlist.txt")


def load_allowlist() -> set[str]:
    allow: set[str] = set()
    with open(ALLOWLIST_FILE) as f:
        for line in f:
            line = line.split("#", 1)[0].strip()
            if line:
                allow.add(line)
    return allow


def _reexec_with_yaml() -> None:
    """Re-run under an interpreter that has PyYAML, if the current one lacks it."""
    if os.environ.get("_STRIP_CATALOG_REEXEC"):
        return
    candidates = [shutil.which(n) for n in ("python3", "python")]
    candidates += ["/opt/homebrew/bin/python3", "/usr/local/bin/python3", "/usr/bin/python3"]
    seen = {os.path.realpath(sys.executable)}
    for py in candidates:
        if not py or not os.path.exists(py):
            continue
        rp = os.path.realpath(py)
        if rp in seen:
            continue
        seen.add(rp)
        try:
            subprocess.run([py, "-c", "import yaml"], check=True, capture_output=True)
        except Exception:
            continue
        os.execve(py, [py, *sys.argv], {**os.environ, "_STRIP_CATALOG_REEXEC": "1"})


def load_catalog(path: str) -> tuple[object, bool]:
    """Load a catalog. Returns (data, is_yaml) — is_yaml drives output format."""
    with open(path) as f:
        text = f.read()
    try:
        import yaml  # noqa: PLC0415 — optional dependency
    except ImportError:
        _reexec_with_yaml()
        try:
            return json.loads(text), False
        except json.JSONDecodeError:
            raise SystemExit(
                f"catalog {path!r} is YAML but PyYAML is not installed for this interpreter "
                f"({sys.executable}). Install it (`python3 -m pip install pyyaml`) or run with "
                "a Python that has it."
            )
    return yaml.safe_load(text), True


def strip_config(value: object, key: str | None, allow: set[str]) -> tuple[bool, object]:
    """Prune a config subtree to allow-listed leaves.

    Returns (keep, pruned). A scalar is kept only if its own leaf `key` is
    allow-listed; scalars inside a list inherit the list's key. Containers are
    kept only when they still hold at least one surviving value.
    """
    if isinstance(value, dict):
        out = {k: pruned for k, v in value.items()
               for keep, pruned in [strip_config(v, k, allow)] if keep}
        return bool(out), out
    if isinstance(value, list):
        out = [pruned for v in value
               for keep, pruned in [strip_config(v, key, allow)] if keep]
        return bool(out), out
    return (key in allow if key is not None else False), value


def _strip_obj(obj: object, allow: set[str]) -> object:
    return strip_config(obj, None, allow)[1]


def sanitize(node: object, allow: set[str]) -> None:
    """Walk the catalog in place, stripping every connector/local config and
    every binding resource wherever they occur (captures, materializations,
    connector-based derivations)."""
    if isinstance(node, dict):
        for k, v in node.items():
            if k in ("connector", "local") and isinstance(v, dict) and isinstance(v.get("config"), (dict, list)):
                v["config"] = _strip_obj(v["config"], allow)
            if k == "bindings" and isinstance(v, list):
                for b in v:
                    if isinstance(b, dict) and isinstance(b.get("resource"), (dict, list)):
                        b["resource"] = _strip_obj(b["resource"], allow)
            sanitize(v, allow)
    elif isinstance(node, list):
        for item in node:
            sanitize(item, allow)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("catalog", help="Flow catalog file (YAML or JSON)")
    parser.add_argument("-o", "--output", help="Output file (default: stdout)")
    args = parser.parse_args()

    allow = load_allowlist()
    catalog, is_yaml = load_catalog(args.catalog)
    if not isinstance(catalog, (dict, list)):
        raise SystemExit(f"catalog {args.catalog!r} did not parse to a mapping/list")

    sanitize(catalog, allow)

    out = open(args.output, "w") if args.output else sys.stdout
    try:
        if is_yaml:
            import yaml
            yaml.safe_dump(catalog, out, sort_keys=False, default_flow_style=False)
        else:
            json.dump(catalog, out, indent=2)
            out.write("\n")
    finally:
        if out is not sys.stdout:
            out.close()


if __name__ == "__main__":
    main()
