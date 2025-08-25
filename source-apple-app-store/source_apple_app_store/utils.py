from datetime import date, datetime

DATETIME_STRING_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
DATE_STRING_FORMAT = "%Y-%m-%d"


def dt_to_str(dt: datetime) -> str:
    return dt.strftime(DATETIME_STRING_FORMAT)


def str_to_dt(string: str) -> datetime:
    return datetime.fromisoformat(string)


def str_to_date(string: str) -> date:
    return datetime.strptime(string, DATE_STRING_FORMAT).date()

def date_to_str(date: date) -> str:
    return date.strftime(DATE_STRING_FORMAT)
