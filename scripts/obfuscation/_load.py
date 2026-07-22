"""Load a Flow catalog / JSON-Schema file as YAML or JSON.

JSON parses with no dependency; YAML needs PyYAML. If the current interpreter
lacks PyYAML, re-exec into one that has it.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys


def _reexec_with_yaml() -> None:
    if os.environ.get("_OBFUSCATION_REEXEC"):
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
        os.execve(py, [py, *sys.argv], {**os.environ, "_OBFUSCATION_REEXEC": "1"})


def load_structured(path: str) -> tuple[object, bool]:
    """Return (data, is_yaml). is_yaml drives output formatting for round-trips."""
    with open(path, encoding="utf-8") as f:
        text = f.read()
    try:
        import yaml  # noqa: PLC0415 — optional dependency
    except ImportError:
        _reexec_with_yaml()
        try:
            return json.loads(text), False
        except json.JSONDecodeError:
            raise SystemExit(
                f"{path!r} is YAML but PyYAML is not installed for this interpreter "
                f"({sys.executable}). Install it (`python3 -m pip install pyyaml`) or run "
                "with a Python that has it."
            )
    return yaml.safe_load(text), True
