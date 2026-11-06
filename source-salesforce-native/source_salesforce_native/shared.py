import re
from datetime import datetime, UTC

from estuary_cdk.http import Headers

VERSION = "62.0"
DATETIME_STRING_FORMAT = "%Y-%m-%dT%H:%M:%S"

# Salesforce record Ids are 15 case-sensitive or 18 case-safe alphanumeric characters.
SALESFORCE_ID_REGEX = re.compile(r"^[a-zA-Z0-9]{15}(?:[a-zA-Z0-9]{3})?$")

# Salesforce recommends retrying transient 500 errors with exponential backoff,
# but limiting retry attempts to avoid burning API credits indefinitely.
MAX_SERVER_ERROR_RETRY_ATTEMPTS = 5


def should_retry(status: int, headers: Headers, body: bytes, attempt: int) -> bool:
    return attempt < MAX_SERVER_ERROR_RETRY_ATTEMPTS


def dt_to_str(dt: datetime) -> str:
    milliseconds = dt.microsecond // 1000
    return dt.strftime(DATETIME_STRING_FORMAT) + f".{milliseconds:03d}Z"


def str_to_dt(string: str) -> datetime:
    return datetime.fromisoformat(string)


# Salesforce's datetimes have millisecond precision. now helps ensure
# we are always working with millisecond precision datetimes.
def now() -> datetime:
    return str_to_dt(dt_to_str(datetime.now(tz=UTC)))


def is_salesforce_id(value: str) -> bool:
    return bool(SALESFORCE_ID_REGEX.fullmatch(value))


def build_snapshot_query(object_name: str, fields: list[str]) -> str:
    return f"SELECT {','.join(fields)} FROM {object_name}"


def build_date_window_query(
        object_name: str,
        fields: list[str],
        cursor_field: str,
        start: datetime,
        end: datetime,
    ) -> str:
    query = f"SELECT {','.join(fields)} FROM {object_name}"
    query += f" WHERE {cursor_field} > {dt_to_str(start)} AND {cursor_field} <= {dt_to_str(end)}"
    query += f" ORDER BY {cursor_field} ASC"

    return query


# build_id_page_query builds a keyset pagination query over the object's primary key: a bounded,
# indexed range scan regardless of how records are distributed over the cursor field.
def build_id_page_query(
        object_name: str,
        fields: list[str],
        cursor_field: str,
        start: datetime,
        end: datetime,
        last_id: str | None = None,
        max_id: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> str:
    for id_bound in (last_id, max_id):
        if id_bound is not None and not is_salesforce_id(id_bound):
            raise ValueError(f"{id_bound} is not a valid Salesforce record Id.")

    query = f"SELECT {','.join(fields)} FROM {object_name}"
    query += f" WHERE {cursor_field} > {dt_to_str(start)} AND {cursor_field} <= {dt_to_str(end)}"

    if last_id:
        query += f" AND Id > '{last_id}'"
    if max_id:
        query += f" AND Id <= '{max_id}'"

    query += " ORDER BY Id ASC"

    if limit is not None:
        query += f" LIMIT {limit}"
    if offset is not None:
        query += f" OFFSET {offset}"

    return query
