from datetime import datetime, date

DATE_FORMAT = "%Y-%m-%d"


def format_date_for_api(dt: datetime | date) -> str:
    if isinstance(dt, datetime):
        return dt.strftime(DATE_FORMAT)
    elif isinstance(dt, date):
        return dt.strftime(DATE_FORMAT)
    else:
        raise ValueError("Input must be a datetime or date object")

def are_same_day(dt1: datetime, dt2: datetime) -> bool:
    return dt1.date() == dt2.date()