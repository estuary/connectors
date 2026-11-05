from datetime import UTC, datetime

API = "https://api.aircall.io/v1"
API_V2 = "https://api.aircall.io/v2"


def now() -> datetime:
    return datetime.now(tz=UTC)


def to_unix(dt: datetime) -> int:
    return int(dt.timestamp())
