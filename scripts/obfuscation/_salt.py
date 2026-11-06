"""Per-user obfuscation salt, persisted under the flowctl config directory.

The salt makes the (public, deterministic) obfuscation non-invertible without
access to this user's config: it is a strong random secret, generated once and
reused, and never printed. Mirrors flowctl's config-dir resolution
(Rust `dirs::config_dir()` + "flowctl").
"""

from __future__ import annotations

import os
import secrets
import sys

_SALT_FILE = "obfuscation_salt"


def _flowctl_config_dir() -> str:
    if sys.platform == "darwin":
        base = os.path.expanduser("~/Library/Application Support")
    elif os.name == "nt":
        base = os.environ.get("APPDATA") or os.path.expanduser("~")
    else:
        xdg = os.environ.get("XDG_CONFIG_HOME")
        base = xdg if xdg and os.path.isabs(xdg) else os.path.expanduser("~/.config")
    return os.path.join(base, "flowctl")


def user_salt() -> str:
    """Return this user's salt, generating and persisting it on first use."""
    path = os.path.join(_flowctl_config_dir(), _SALT_FILE)
    try:
        with open(path, encoding="utf-8") as f:
            existing = f.read().strip()
        if existing:
            return existing
    except FileNotFoundError:
        pass

    os.makedirs(os.path.dirname(path), exist_ok=True)
    salt = secrets.token_hex(32)
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        # A concurrent run created it first; adopt theirs.
        with open(path, encoding="utf-8") as f:
            return f.read().strip()
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(salt)
    return salt
