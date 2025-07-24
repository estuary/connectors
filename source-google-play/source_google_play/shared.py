from datetime import datetime

DATETIME_STRING_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def dt_to_str(dt: datetime) -> str:
    return dt.strftime(DATETIME_STRING_FORMAT)


def str_to_dt(string: str) -> datetime:
    return datetime.fromisoformat(string)


def start_of_month(dt: datetime) -> datetime:
    """Return a datetime object representing the first day of the month of the given date."""
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def next_month(dt: datetime) -> datetime:
    """Return a datetime object representing the first day of the month after the given date."""
    this_month = start_of_month(dt)

    if this_month.month == 12:
        return this_month.replace(year=this_month.year + 1, month=1)
    else:
        return this_month.replace(month=this_month.month + 1)
