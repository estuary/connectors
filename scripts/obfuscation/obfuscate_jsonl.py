#!/usr/bin/env python3
"""Obfuscate the field values of a JSONL document collection.

Every leaf value in each document is replaced with obfuscated data, EXCEPT:

  * ``/_meta/uuid``   — the document UUID (Flow protocol metadata)
  * ``/_meta/source`` — the source metadata subtree (kept intact)
  * ``/_meta/op``     — the change operation (c/u/d)
  * the collection's key fields — taken from the ``key`` array of the matching
    collection in a Flow catalog file.

Key and ``_meta`` fields are preserved so the obfuscated documents remain valid
and reducible: the key still identifies each document and the UUID still drives
reduction ordering.

Obfuscation is deterministic (the same input value always maps to the same
output) so referential relationships across documents survive, and it preserves
the *shape* of values — string length and per-character class, number sign and
magnitude, and RFC3339 date-times stay valid date-times.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys

from obfuscate_common import obfuscate_leaf


def _reexec_with_yaml() -> None:
    """Re-run this script under an interpreter that has PyYAML, if we can find one.

    JSON is a subset of YAML, so JSON catalogs never need this — but YAML catalogs
    do, and the interpreter the user invoked may not be the one PyYAML is installed
    into. Rather than make the user hunt for the right ``python3``, locate one and
    hand off to it. Guarded by an env var so we never loop.
    """
    if os.environ.get("_OBFUSCATE_REEXEC"):
        return
    candidates = [shutil.which(n) for n in ("python3", "python")]
    candidates += ["/opt/homebrew/bin/python3", "/usr/local/bin/python3", "/usr/bin/python3"]
    current = os.path.realpath(sys.executable)
    seen = {current}
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
        os.execve(py, [py, *sys.argv], {**os.environ, "_OBFUSCATE_REEXEC": "1"})


def load_catalog(catalog_path: str) -> object:
    """Load a Flow catalog as YAML if PyYAML is available, else as JSON."""
    with open(catalog_path) as f:
        text = f.read()
    try:
        import yaml  # noqa: PLC0415 — optional dependency, imported lazily
    except ImportError:
        _reexec_with_yaml()  # may not return (execs a better interpreter)
        try:
            return json.loads(text)  # JSON is valid YAML; works dependency-free
        except json.JSONDecodeError:
            raise SystemExit(
                f"catalog {catalog_path!r} is YAML but PyYAML is not installed for "
                f"this interpreter ({sys.executable}).\n"
                "Install it (`python3 -m pip install pyyaml`) or run the script with "
                "a Python that has it."
            )
    return yaml.safe_load(text)


def parse_json_pointer(pointer: str) -> tuple[str, ...]:
    """Convert a JSON pointer (e.g. ``/foo/bar~1baz``) into a token tuple."""
    if pointer in ("", "/"):
        return ()
    if not pointer.startswith("/"):
        raise ValueError(f"key pointer must start with '/': {pointer!r}")
    tokens = pointer.split("/")[1:]
    return tuple(t.replace("~1", "/").replace("~0", "~") for t in tokens)


def load_preserved_paths(catalog_path: str, collection: str | None) -> set[tuple[str, ...]]:
    """Return the set of token-paths to preserve for the selected collection."""
    catalog = load_catalog(catalog_path)
    if not isinstance(catalog, dict):
        raise SystemExit(f"catalog {catalog_path!r} did not parse to a mapping")

    collections = catalog.get("collections") or {}
    if not collections:
        raise SystemExit(f"no collections found in catalog {catalog_path!r}")

    if collection is None:
        if len(collections) == 1:
            collection = next(iter(collections))
        else:
            names = "\n  ".join(sorted(collections))
            raise SystemExit(
                "catalog defines multiple collections; pass --collection with one of:\n  "
                + names
            )
    elif collection not in collections:
        names = "\n  ".join(sorted(collections))
        raise SystemExit(f"collection {collection!r} not in catalog. Available:\n  " + names)

    spec = collections[collection]
    key = spec.get("key")
    if not key:
        raise SystemExit(f"collection {collection!r} has no 'key' field")

    preserved = {parse_json_pointer(p) for p in key}
    preserved.add(("_meta", "uuid"))
    preserved.add(("_meta", "source"))
    preserved.add(("_meta", "op"))
    return preserved


def obfuscate(value: object, path: tuple[str, ...], preserved: set[tuple[str, ...]], salt: str) -> object:
    if path in preserved:
        return value  # preserve this value and its entire subtree
    if isinstance(value, dict):
        return {k: obfuscate(v, path + (k,), preserved, salt) for k, v in value.items()}
    if isinstance(value, list):
        return [obfuscate(v, path + (str(i),), preserved, salt) for i, v in enumerate(value)]
    return obfuscate_leaf(value, salt)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("jsonl", help="Input JSONL file ('-' for stdin)")
    parser.add_argument("catalog", help="Flow collection catalog file (YAML or JSON)")
    parser.add_argument("-c", "--collection", help="Collection name (required if the catalog has more than one)")
    parser.add_argument("-o", "--output", help="Output JSONL file (default: stdout)")
    parser.add_argument("--salt", default="obfuscate", help="Salt for deterministic obfuscation (default: 'obfuscate')")
    args = parser.parse_args()

    preserved = load_preserved_paths(args.catalog, args.collection)

    infile = sys.stdin if args.jsonl == "-" else open(args.jsonl)
    outfile = open(args.output, "w") if args.output else sys.stdout
    try:
        for lineno, line in enumerate(infile, 1):
            line = line.strip()
            if not line:
                continue
            try:
                doc = json.loads(line)
            except json.JSONDecodeError as e:
                raise SystemExit(f"{args.jsonl}:{lineno}: invalid JSON: {e}")
            obfuscated = obfuscate(doc, (), preserved, args.salt)
            outfile.write(json.dumps(obfuscated, separators=(",", ":")) + "\n")
    finally:
        if infile is not sys.stdin:
            infile.close()
        if outfile is not sys.stdout:
            outfile.close()


if __name__ == "__main__":
    main()
