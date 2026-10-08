import re
from datetime import UTC, datetime

# The shape `to_rfc3339_ms` produces.
RFC3339_MS = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z")


def to_rfc3339_ms(dt: datetime) -> str:
    """Format a datetime the way Okta stamps `published`: UTC, millisecond, `Z`."""
    dt = dt.astimezone(UTC)
    return f"{dt:%Y-%m-%dT%H:%M:%S}.{dt.microsecond // 1000:03d}Z"


def floor_to_ms(dt: datetime) -> datetime:
    return dt.replace(microsecond=dt.microsecond // 1000 * 1000)
