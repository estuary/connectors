#!/usr/bin/env python3
"""Sanitize a Flow catalog so it exposes nothing about the customer.

Removes / obfuscates every customer-identifying part of a catalog:

  * Connector endpoint configs and binding resources are stripped to an
    allow-list of customer-agnostic knobs (config_allowlist.txt); everything
    else (hosts, credentials, accounts, database/schema/table names, buckets,
    regions, ...) is removed. Strip-by-default: unknown keys are dropped.
  * `local.command` is removed.
  * Collection-schema annotations that can embed literal data — title,
    description, $comment, examples, example, default, const, enum — are removed.
  * Derivation code — transform lambdas, SQL migrations, TypeScript modules — is
    removed, and transform names are obfuscated.
  * Task / collection / materialization names (including the tenant prefix) and
    every reference to them are obfuscated segment-by-segment, so the catalog
    keeps working internal references but reveals no real names.

Structural fields (connector image, schema shape/keys, binding structure) are
kept. YAML in -> YAML out; JSON in -> JSON out.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import string
import sys

from _load import load_structured
from _salt import user_salt

ALLOWLIST_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config_allowlist.txt")

# Schema annotation keys that can carry literal customer values.
ANNOTATION_KEYS = {"title", "description", "$comment", "comment", "examples", "example", "default", "const", "enum"}
# JSON-Schema keys whose values are themselves schemas (for annotation stripping).
_SUB = {"items", "additionalProperties", "additionalItems", "not", "if", "then", "else",
        "contains", "propertyNames", "unevaluatedItems", "unevaluatedProperties"}
_SUB_MAP = {"properties", "patternProperties", "$defs", "definitions", "dependentSchemas"}
_SUB_LIST = {"allOf", "anyOf", "oneOf", "prefixItems"}
# Catalog keys whose values reference a collection/capture by name.
_REF_KEYS = {"target", "source", "collection", "sourceCapture"}


def load_allowlist() -> set[str]:
    allow: set[str] = set()
    with open(ALLOWLIST_FILE, encoding="utf-8") as f:
        for line in f:
            line = line.split("#", 1)[0].strip()
            if line:
                allow.add(line)
    return allow


# ---- config / resource stripping ------------------------------------------

def strip_config(value: object, key: str | None, allow: set[str]) -> tuple[bool, object]:
    """Prune a config subtree to allow-listed scalar leaves (recursively)."""
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


def sanitize_configs(node: object, allow: set[str]) -> None:
    if isinstance(node, dict):
        for k, v in node.items():
            if k in ("connector", "local") and isinstance(v, dict) and isinstance(v.get("config"), (dict, list)):
                v["config"] = _strip_obj(v["config"], allow)
            if k == "local" and isinstance(v, dict):
                v.pop("command", None)
            if k == "bindings" and isinstance(v, list):
                for b in v:
                    if isinstance(b, dict) and isinstance(b.get("resource"), (dict, list)):
                        b["resource"] = _strip_obj(b["resource"], allow)
            sanitize_configs(v, allow)
    elif isinstance(node, list):
        for item in node:
            sanitize_configs(item, allow)


# ---- schema annotation + derivation stripping -----------------------------

def strip_schema_annotations(node: object) -> None:
    """Remove annotation keys from a JSON schema, recursing only into subschema
    positions (so property *names* like a field literally called `default` are
    never mistaken for annotations)."""
    if not isinstance(node, dict):
        if isinstance(node, list):
            for item in node:
                strip_schema_annotations(item)
        return
    for a in ANNOTATION_KEYS:
        node.pop(a, None)
    for k in _SUB:
        if k in node:
            strip_schema_annotations(node[k])
    for k in _SUB_MAP:
        if isinstance(node.get(k), dict):
            for sub in node[k].values():
                strip_schema_annotations(sub)
    for k in _SUB_LIST:
        if isinstance(node.get(k), list):
            for sub in node[k]:
                strip_schema_annotations(sub)


def strip_derivation(derive: object, salt: str) -> None:
    if not isinstance(derive, dict):
        return

    def scrub(n: object) -> None:
        if isinstance(n, dict):
            for code_key in ("lambda", "migrations", "module"):
                n.pop(code_key, None)
            for v in n.values():
                scrub(v)
        elif isinstance(n, list):
            for x in n:
                scrub(x)

    scrub(derive)
    for t in derive.get("transforms") or []:
        if isinstance(t, dict) and isinstance(t.get("name"), str):
            t["name"] = _obf_segment(t["name"], salt)


# ---- name obfuscation ------------------------------------------------------

def _rng(value: str, salt: str) -> random.Random:
    return random.Random(int.from_bytes(hashlib.sha256(f"{salt}\0{value!r}".encode()).digest(), "big"))


def _obf_segment(seg: str, salt: str) -> str:
    """Obfuscate one name segment, keeping it a valid Flow-name segment: ASCII
    letters/digits are scrambled within class; `-`, `_`, `.` and other
    separators are kept, so the result stays a legal name of the same shape."""
    r = _rng(seg, salt)
    out = []
    for ch in seg:
        if ch.isdigit():
            out.append(r.choice(string.digits))
        elif "a" <= ch <= "z":
            out.append(r.choice(string.ascii_lowercase))
        elif "A" <= ch <= "Z":
            out.append(r.choice(string.ascii_uppercase))
        else:
            out.append(ch)
    return "".join(out)


def _obf_name(name: str, salt: str) -> str:
    # Segment-by-segment so hierarchical prefixes stay consistent across
    # references (a full name and a prefix of it map compatibly).
    return "/".join(_obf_segment(s, salt) for s in name.split("/"))


def _rename_refs(node: object, salt: str) -> None:
    if isinstance(node, dict):
        for k, v in node.items():
            if k in _REF_KEYS:
                if isinstance(v, str):
                    node[k] = _obf_name(v, salt)
                elif isinstance(v, dict):
                    for nk in ("name", "capture"):
                        if isinstance(v.get(nk), str):
                            v[nk] = _obf_name(v[nk], salt)
            _rename_refs(v, salt)
    elif isinstance(node, list):
        for item in node:
            _rename_refs(item, salt)


def obfuscate_names(catalog: dict, salt: str) -> None:
    for section in ("captures", "collections", "materializations", "tests", "storageMappings"):
        entries = catalog.get(section)
        if isinstance(entries, dict):
            catalog[section] = {_obf_name(str(name), salt): spec for name, spec in entries.items()}
    _rename_refs(catalog, salt)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("catalog", help="Flow catalog file (YAML or JSON)")
    parser.add_argument("-o", "--output", help="Output file (default: stdout)")
    args = parser.parse_args()

    allow = load_allowlist()
    salt = user_salt()
    catalog, is_yaml = load_structured(args.catalog)
    if not isinstance(catalog, (dict, list)):
        raise SystemExit(f"catalog {args.catalog!r} did not parse to a mapping/list")

    sanitize_configs(catalog, allow)
    if isinstance(catalog, dict):
        for spec in (catalog.get("collections") or {}).values():
            if isinstance(spec, dict):
                for sk in ("schema", "writeSchema", "readSchema"):
                    if sk in spec:
                        strip_schema_annotations(spec[sk])
                if "derive" in spec:
                    strip_derivation(spec["derive"], salt)
        obfuscate_names(catalog, salt)

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
