from datetime import datetime, UTC


def now() -> datetime:
    """
    Returns the current time with microseconds removed.
    """
    return datetime.now(tz=UTC).replace(microsecond=0)
