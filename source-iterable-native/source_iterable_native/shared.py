from datetime import datetime, UTC

BASE_URL = "https://api.iterable.com/api"
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

def ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=UTC)


def dt_to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def str_to_dt(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def dt_to_str(dt: datetime) -> str:
    """Format datetime as ISO string with millisecond precision (e.g., 2024-01-15T10:30:00.000Z)."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def now() -> datetime:
    """Return current UTC time truncated to millisecond precision."""
    dt = datetime.now(tz=UTC)
    return dt.replace(microsecond=(dt.microsecond // 1000) * 1000)
