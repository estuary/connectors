from datetime import datetime

DATETIME_STRING_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def dt_to_str(dt: datetime) -> str:
    return dt.strftime(DATETIME_STRING_FORMAT)


def str_to_dt(dt_str: str) -> datetime:
    return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
