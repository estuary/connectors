from datetime import datetime

def str_to_dt(string: str) -> datetime:
    normalized = string.replace('.', ':')
    return datetime.fromisoformat(normalized)


def is_datetime_format(s: str) -> bool:
    try:
        str_to_dt(s)
        return True
    except ValueError:
        return False
