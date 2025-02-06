from datetime import datetime


DATETIME_STRING_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
DATE_STRING_FORMAT = "%Y-%m-%d"


def dt_to_str(dt: datetime) -> str:
    return dt.strftime(DATETIME_STRING_FORMAT)


def dt_to_date_str(dt: datetime) -> str:
    return dt.strftime(DATE_STRING_FORMAT)


def str_to_dt(string: str) -> datetime:
    return datetime.fromisoformat(string)
