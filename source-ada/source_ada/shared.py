from datetime import datetime, UTC


def dt_to_str(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def str_to_dt(string: str) -> datetime:
    return datetime.fromisoformat(string)


def now() -> datetime:
    """
    Returns the current time.
    """
    return datetime.now(tz=UTC)
