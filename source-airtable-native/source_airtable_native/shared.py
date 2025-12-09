from datetime import datetime, UTC


def dt_to_str(dt: datetime) -> str:
    return dt.isoformat(timespec='seconds').replace("+00:00", "Z")


def str_to_dt(string: str) -> datetime:
    return datetime.fromisoformat(string)


def now() -> datetime:
    """
    Returns the current time with microseconds removed.
    """
    return datetime.now(tz=UTC).replace(microsecond=0)
