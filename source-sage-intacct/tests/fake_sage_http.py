"""A fake of the Sage Intacct HTTP API for tests. It makes no network call."""

import json
from datetime import datetime
from typing import Any

import xmltodict

from source_sage_intacct.sage import PAGE_SIZE

TIMESTAMP_FORMAT = "%m/%d/%Y %H:%M:%S"

# The fields of an object that is not listed in DEFAULT_LOOKUP's place.
LOOKUPS: dict[str, list[tuple[str, str]]] = {
    "GLDETAIL": [
        ("RECORDNO", "TEXT"),
        ("BATCHKEY", "INTEGER"),
        ("ENTRY_DATE", "DATE"),
        ("WHENCREATED", "DATE"),
        ("WHENMODIFIED", "TIMESTAMP"),
    ],
}
DEFAULT_LOOKUP = [
    ("RECORDNO", "INTEGER"),
    ("BATCH_DATE", "DATE"),
    ("ENTRY_DATE", "DATE"),
    ("WHENCREATED", "TIMESTAMP"),
    ("WHENMODIFIED", "TIMESTAMP"),
]


def envelope(data: str) -> bytes:
    return f"""<response><operation>
        <authentication>
          <status>success</status>
          <userid>user_id</userid>
          <sessiontimeout>2099-01-01T00:00:00+00:00</sessiontimeout>
        </authentication>
        <result><status>success</status><data>{data}</data></result>
    </operation></response>""".encode()


def typed(field: str, value: str) -> Any:
    if field in ("WHENMODIFIED", "WHENCREATED", "ACCESSTIME") and " " in value:
        return datetime.strptime(f"{value} +0000", f"{TIMESTAMP_FORMAT} %z")
    if field.endswith("_DATE") or field == "WHENCREATED":
        return datetime.strptime(f"{value} +0000", "%m/%d/%Y %z")
    return int(value) if value.isdigit() else value


def matches(row: dict[str, str], condition: dict[str, Any]) -> bool:
    ((operator, body),) = condition.items()
    if operator == "and":
        return all(
            matches(row, {op: part})
            for op, parts in body.items()
            for part in (parts if isinstance(parts, list) else [parts])
        )

    value = row.get(body["field"], "")
    if operator == "isnull":
        return value == ""
    if value == "":
        return False
    left, right = typed(body["field"], value), typed(body["field"], body["value"])
    return {
        "greaterthan": left > right,
        "greaterthanorequalto": left >= right,
        "equalto": left == right,
    }[operator]


class FakeSageHTTP:
    """Answers Sage requests from rows in memory, as the real API does: XML
    for the session, `lookup` and permission requests, and a JSON list for
    each query, filtered, sorted and cut to one page."""

    def __init__(self, rows: dict[str, list[dict[str, str]]] | None = None):
        self.rows = rows or {}

    async def request(self, log, url, method, headers, form) -> bytes:
        function = xmltodict.parse(form)["request"]["operation"]["content"]["function"]
        (name,) = (key for key in function if not key.startswith("@"))
        body = function[name] or {}
        match name:
            case "getAPISession":
                return envelope(
                    "<api><sessionid>s</sessionid><endpoint>e</endpoint></api>"
                )
            case "readByName":
                return envelope("<USERINFO><RECORDNO>1</RECORDNO></USERINFO>")
            case "readUserFormatting":
                return envelope(
                    "<userformatting><locale>en_US</locale>"
                    "<dateformat>/mdY</dateformat><gmtoffset>+00:00</gmtoffset>"
                    "<clock>24</clock></userformatting>"
                )
            case "lookup":
                fields = LOOKUPS.get(body["object"], DEFAULT_LOOKUP)
                return envelope(
                    "<Type><Fields>"
                    + "".join(
                        f"<Field><ID>{id}</ID><DATATYPE>{datatype}</DATATYPE></Field>"
                        for id, datatype in fields
                    )
                    + "</Fields></Type>"
                )
            case "query":
                # The permission probe is the only query that is not JSON.
                return envelope("")
        raise AssertionError(f"unexpected request {name}")

    async def request_stream(self, log, url, method, headers, form):
        query = xmltodict.parse(form)["request"]["operation"]["content"]["function"][
            "query"
        ]
        assert int(query["pagesize"]) == PAGE_SIZE

        rows = [
            row
            for row in self.rows.get(query["object"], [])
            if "filter" not in query or matches(row, query["filter"])
        ]
        order_field = query["orderby"]["order"]["field"]
        rows.sort(key=lambda row: typed(order_field, row[order_field]))
        page = json.dumps(rows[:PAGE_SIZE]).encode()

        async def body():
            yield page

        return {"Content-Type": "application/json"}, body
