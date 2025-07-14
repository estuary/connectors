from datetime import datetime, UTC

VERSION = "62.0"
DATETIME_STRING_FORMAT = "%Y-%m-%dT%H:%M:%S"


def dt_to_str(dt: datetime) -> str:
    milliseconds = dt.microsecond // 1000
    return dt.strftime(DATETIME_STRING_FORMAT) + f".{milliseconds:03d}Z"


def str_to_dt(string: str) -> datetime:
    return datetime.fromisoformat(string)


# Salesforce's datetimes have millisecond precision. now helps ensure
# we are always working with millisecond precision datetimes.
def now() -> datetime:
    return str_to_dt(dt_to_str(datetime.now(tz=UTC)))


def build_query(
        object_name: str, 
        fields: list[str],
        cursor_field: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> str:
    query = f"SELECT {','.join(fields)} FROM {object_name}"

    if cursor_field and (start or end):
        query += f" WHERE"
        if start:
            query += f" {cursor_field} > {dt_to_str(start)}"
        if start and end:
            query += " AND"
        if end:
            query += f" {cursor_field} <= {dt_to_str(end)}"
        query += f" ORDER BY {cursor_field} ASC"

    return query
