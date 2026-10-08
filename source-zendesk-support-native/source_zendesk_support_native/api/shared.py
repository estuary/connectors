from datetime import datetime, timedelta, UTC

CHECKPOINT_INTERVAL = 1000
CURSOR_PAGINATION_PAGE_SIZE = 100
# Zendesk errors out if a start or end time parameter is 60 seconds or less in the past. 
TIME_PARAMETER_DELAY = timedelta(seconds=61)
# Zendesk's API is eventually consistent: a record can become queryable seconds or
# minutes after its timestamp, after we've already seen records with later timestamps.
# Since incremental cursors are high-water marks, anything that surfaces below one
# would be filtered out and lost forever. To avoid that, we never advance a cursor
# over records younger than INCREMENTAL_LAG, giving late arrivals time to appear
# before the cursor moves past their timestamp.
INCREMENTAL_LAG = timedelta(minutes=5)
DATETIME_STRING_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
INCREMENTAL_TIME_EXPORT_REQ_PER_MIN_LIMIT = 10


def url_base(subdomain: str) -> str:
    return f"https://{subdomain}.zendesk.com/api/v2"


def dt_to_s(dt: datetime) -> int:
    return int(dt.timestamp())


def s_to_dt(s: int) -> datetime:
    return datetime.fromtimestamp(s, tz=UTC)


def dt_to_str(dt: datetime) -> str:
    return dt.strftime(DATETIME_STRING_FORMAT)


def str_to_dt(string: str) -> datetime:
    return datetime.fromisoformat(string)


def is_timestamp(string: str) -> bool:
    try:
        s_to_dt(int(string))
        return True
    except ValueError:
        return False
