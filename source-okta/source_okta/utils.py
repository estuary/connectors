from datetime import UTC, datetime


def to_rfc3339_ms(dt: datetime) -> str:
    """Format a datetime the way Okta stamps `published`: UTC, millisecond, `Z`."""
    dt = dt.astimezone(UTC)
    return f"{dt:%Y-%m-%dT%H:%M:%S}.{dt.microsecond // 1000:03d}Z"


def floor_to_ms(dt: datetime) -> datetime:
    return dt.replace(microsecond=dt.microsecond // 1000 * 1000)
