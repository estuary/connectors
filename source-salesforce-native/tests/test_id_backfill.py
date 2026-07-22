import json
import logging
from datetime import datetime, timedelta, UTC

import pytest

from source_salesforce_native.api import (
    BULK_ID_PAGE_LIMIT,
    REST_ID_PAGE_LIMIT,
    _id_execution_wrapper,
    backfill_incremental_resources,
)
from source_salesforce_native.models import (
    CursorFields,
    FieldDetailsDict,
    SalesforceDataSource,
    SalesforceRecord,
    ValidationContext,
    create_salesforce_model,
)
from source_salesforce_native.rest_query_manager import RestQueryManager
from source_salesforce_native.shared import build_id_page_query, dt_to_str, is_salesforce_id

_LOG = logging.getLogger(__name__)

START_DATE = datetime(1999, 2, 3, tzinfo=UTC)
CUTOFF = datetime(2026, 7, 1, tzinfo=UTC)
WINDOW_SIZE = timedelta(days=18250)
MODSTAMP = "2026-06-01T00:00:00.000Z"
INSTANCE_URL = "https://instance.example.com"

FIELDS = FieldDetailsDict.model_validate({
    "Id": {"soapType": "tns:ID", "calculated": False, "custom": False},
    "SystemModstamp": {"soapType": "xsd:dateTime", "calculated": False, "custom": False},
    "Name": {"soapType": "xsd:string", "calculated": False, "custom": False},
})

MODEL = create_salesforce_model("Account", FIELDS)
REST_CONTEXT = ValidationContext(SalesforceDataSource.REST_API)


def _id(n: int) -> str:
    return f"001{n:012d}AAA"


def _record(n: int) -> SalesforceRecord:
    return MODEL.model_validate(
        {"Id": _id(n), "SystemModstamp": MODSTAMP, "Name": f"record {n}"},
        context=REST_CONTEXT,
    )


async def _collect(gen) -> list:
    return [item async for item in gen]


def test_is_salesforce_id():
    assert is_salesforce_id("001000000000001")
    assert is_salesforce_id("001000000000001AAA")
    assert is_salesforce_id("a0B4x00000FaKeI")
    # Legacy datetime page cursors are never classified as Ids.
    assert not is_salesforce_id(dt_to_str(CUTOFF))
    assert not is_salesforce_id("")
    assert not is_salesforce_id("001-00000000001")
    assert not is_salesforce_id("0010000000000012")
    assert not is_salesforce_id("001000000000001AAAX")


def test_build_id_page_query():
    fields = ["Id", "SystemModstamp", "Name"]

    assert build_id_page_query("Account", fields, CursorFields.SYSTEM_MODSTAMP, START_DATE, CUTOFF, limit=2000) == (
        "SELECT Id,SystemModstamp,Name FROM Account"
        " WHERE SystemModstamp > 1999-02-03T00:00:00.000Z AND SystemModstamp <= 2026-07-01T00:00:00.000Z"
        " ORDER BY Id ASC LIMIT 2000"
    )

    assert build_id_page_query("Account", fields, CursorFields.SYSTEM_MODSTAMP, START_DATE, CUTOFF, last_id=_id(5), limit=2000) == (
        "SELECT Id,SystemModstamp,Name FROM Account"
        " WHERE SystemModstamp > 1999-02-03T00:00:00.000Z AND SystemModstamp <= 2026-07-01T00:00:00.000Z"
        f" AND Id > '{_id(5)}'"
        " ORDER BY Id ASC LIMIT 2000"
    )

    assert build_id_page_query("Account", fields, CursorFields.SYSTEM_MODSTAMP, START_DATE, CUTOFF, last_id=_id(5), max_id=_id(9)) == (
        "SELECT Id,SystemModstamp,Name FROM Account"
        " WHERE SystemModstamp > 1999-02-03T00:00:00.000Z AND SystemModstamp <= 2026-07-01T00:00:00.000Z"
        f" AND Id > '{_id(5)}' AND Id <= '{_id(9)}'"
        " ORDER BY Id ASC"
    )

    assert build_id_page_query("Account", ["Id"], CursorFields.SYSTEM_MODSTAMP, START_DATE, CUTOFF, limit=1, offset=1999) == (
        "SELECT Id FROM Account"
        " WHERE SystemModstamp > 1999-02-03T00:00:00.000Z AND SystemModstamp <= 2026-07-01T00:00:00.000Z"
        " ORDER BY Id ASC LIMIT 1 OFFSET 1999"
    )

    with pytest.raises(ValueError):
        build_id_page_query("Account", fields, CursorFields.SYSTEM_MODSTAMP, START_DATE, CUTOFF, last_id="Robert'); DROP")


async def _record_stream(records: list[SalesforceRecord]):
    for record in records:
        yield record


@pytest.mark.asyncio
async def test_id_execution_wrapper_full_page():
    records = [_record(n) for n in range(1, 6)]

    items = await _collect(_id_execution_wrapper(_record_stream(records), checkpoint_interval=2, limit=5))

    assert [i if isinstance(i, str) else i.Id for i in items] == [
        _id(1), _id(2), _id(2), _id(3), _id(4), _id(4), _id(5), _id(5),
    ]
    # A full page ends with a trailing cursor so the CDK invokes the next page.
    assert isinstance(items[-1], str)


@pytest.mark.asyncio
async def test_id_execution_wrapper_short_page():
    records = [_record(n) for n in range(1, 6)]

    items = await _collect(_id_execution_wrapper(_record_stream(records), checkpoint_interval=2, limit=10))

    # Intra-page checkpoints are still emitted, but a short page must end on a document
    # so the CDK considers the backfill complete.
    assert [i for i in items if isinstance(i, str)] == [_id(2), _id(4)]
    assert isinstance(items[-1], SalesforceRecord)


@pytest.mark.asyncio
async def test_id_execution_wrapper_never_ends_on_interval_checkpoint():
    records = [_record(n) for n in range(1, 5)]

    items = await _collect(_id_execution_wrapper(_record_stream(records), checkpoint_interval=2, limit=10))

    # 4 records with interval 2: the stream ends exactly on a checkpoint boundary, but the
    # wrapper must not end a short page with a cursor.
    assert isinstance(items[-1], SalesforceRecord)


@pytest.mark.asyncio
async def test_id_execution_wrapper_empty():
    items = await _collect(_id_execution_wrapper(_record_stream([]), checkpoint_interval=2, limit=10))
    assert items == []


class FakeBulkManager:
    def __init__(self, records: list[SalesforceRecord]):
        self.records = records
        self.queries: list[str] = []

    async def execute(self, name, query, model_cls):
        self.queries.append(query)
        for record in self.records:
            yield record


class FakeHTTP:
    def __init__(self, responses: list[str]):
        self.responses = list(responses)
        self.requests: list[tuple[str, dict | None]] = []

    async def request(self, log, url, method="GET", params=None, json=None, should_retry=None):
        self.requests.append((url, params))
        return self.responses.pop(0)


def _query_response(records: list[dict], done: bool = True, next_records_url: str | None = None) -> str:
    return json.dumps({
        "totalSize": len(records),
        "done": done,
        "records": records,
        "nextRecordsUrl": next_records_url,
    })


def _rest_record(n: int, modstamp: str = MODSTAMP, **extra) -> dict:
    return {
        "attributes": {"type": "Account"},
        "Id": _id(n),
        "SystemModstamp": modstamp,
        "Name": f"record {n}",
        **extra,
    }


def _backfill(http, is_supported_by_bulk_api, bulk_manager, rest_manager, page):
    return backfill_incremental_resources(
        http,
        is_supported_by_bulk_api,
        bulk_manager,
        rest_manager,
        INSTANCE_URL,
        "Account",
        FIELDS,
        MODEL,
        WINDOW_SIZE,
        START_DATE,
        _LOG,
        page,
        CUTOFF,
        False,  # is_connector_initiated
    )


def _bulk_backfill(bulk_manager: FakeBulkManager, page):
    return _backfill(None, True, bulk_manager, None, page)


@pytest.mark.asyncio
async def test_backfill_new_backfills_use_id_pagination():
    manager = FakeBulkManager([_record(1), _record(2)])

    items = await _collect(_bulk_backfill(manager, page=None))

    assert manager.queries == [
        "SELECT Id,SystemModstamp,Name FROM Account"
        " WHERE SystemModstamp > 1999-02-03T00:00:00.000Z AND SystemModstamp <= 2026-07-01T00:00:00.000Z"
        f" ORDER BY Id ASC LIMIT {BULK_ID_PAGE_LIMIT}"
    ]
    # A short page ends on a document, completing the backfill.
    assert [i.Id for i in items] == [_id(1), _id(2)]


@pytest.mark.asyncio
async def test_backfill_id_page_cursor_resumes_id_pagination():
    manager = FakeBulkManager([_record(2)])

    await _collect(_bulk_backfill(manager, page=_id(1)))

    assert len(manager.queries) == 1
    assert f"AND Id > '{_id(1)}'" in manager.queries[0]
    assert "ORDER BY Id ASC" in manager.queries[0]


@pytest.mark.asyncio
async def test_backfill_datetime_page_cursor_uses_date_windows():
    manager = FakeBulkManager([_record(1), _record(2)])
    page = dt_to_str(datetime(2026, 1, 1, tzinfo=UTC))

    items = await _collect(_bulk_backfill(manager, page=page))

    assert manager.queries == [
        "SELECT Id,SystemModstamp,Name FROM Account"
        " WHERE SystemModstamp > 2026-01-01T00:00:00.000Z AND SystemModstamp <= 2026-07-01T00:00:00.000Z"
        " ORDER BY SystemModstamp ASC"
    ]
    # The legacy strategy checkpoints datetime cursors.
    assert items[-1] == MODSTAMP


@pytest.mark.asyncio
async def test_backfill_rest_full_page_yields_boundary():
    http = FakeHTTP([
        # Boundary probe locates the Id of the last record in the page.
        _query_response([{"attributes": {"type": "Account"}, "Id": _id(2)}]),
        _query_response([_rest_record(1), _rest_record(2)]),
    ])
    rest_manager = RestQueryManager(http, _LOG, INSTANCE_URL)

    items = await _collect(_backfill(http, False, None, rest_manager, page=None))

    assert [i if isinstance(i, str) else i.Id for i in items] == [_id(1), _id(2), _id(2)]
    assert isinstance(items[-1], str)

    _, probe_params = http.requests[0]
    assert probe_params is not None and f"LIMIT 1 OFFSET {REST_ID_PAGE_LIMIT - 1}" in probe_params["q"]

    _, data_params = http.requests[1]
    assert data_params is not None
    assert f"Id <= '{_id(2)}'" in data_params["q"]
    assert "LIMIT" not in data_params["q"]


@pytest.mark.asyncio
async def test_backfill_rest_final_page_has_no_boundary():
    http = FakeHTTP([
        # An empty probe means fewer than a full page of records remain.
        _query_response([]),
        _query_response([_rest_record(1)]),
    ])
    rest_manager = RestQueryManager(http, _LOG, INSTANCE_URL)

    items = await _collect(_backfill(http, False, None, rest_manager, page=None))

    # The final page ends on a document, completing the backfill.
    assert [i.Id for i in items] == [_id(1)]

    _, data_params = http.requests[1]
    assert data_params is not None
    assert "Id <=" not in data_params["q"]
    assert "LIMIT" not in data_params["q"]


@pytest.mark.asyncio
async def test_backfill_rest_empty_page():
    http = FakeHTTP([
        _query_response([]),
        _query_response([]),
    ])
    rest_manager = RestQueryManager(http, _LOG, INSTANCE_URL)

    items = await _collect(_backfill(http, False, None, rest_manager, page=None))

    assert items == []


CHUNKED_FIELDS = FieldDetailsDict.model_validate({
    "Id": {"soapType": "tns:ID", "calculated": False, "custom": False},
    "SystemModstamp": {"soapType": "xsd:dateTime", "calculated": False, "custom": False},
    "Name": {"soapType": "xsd:string", "calculated": False, "custom": False},
    "Website": {"soapType": "xsd:string", "calculated": False, "custom": False},
})

CHUNKED_MODEL = create_salesforce_model("Account", CHUNKED_FIELDS)

CHUNK_QUERIES = [
    "SELECT Id,SystemModstamp,Name FROM Account ORDER BY Id ASC",
    "SELECT Id,SystemModstamp,Website FROM Account ORDER BY Id ASC",
]


@pytest.mark.asyncio
async def test_rest_execute_merges_chunked_queries():
    http = FakeHTTP([
        _query_response([_rest_record(1), _rest_record(2)]),
        _query_response([
            {"attributes": {"type": "Account"}, "Id": _id(1), "SystemModstamp": MODSTAMP, "Website": "https://one.example.com"},
            {"attributes": {"type": "Account"}, "Id": _id(2), "SystemModstamp": MODSTAMP, "Website": "https://two.example.com"},
        ]),
    ])
    manager = RestQueryManager(http, _LOG, INSTANCE_URL)

    records = await _collect(
        manager.execute("Account", CHUNK_QUERIES, CHUNKED_MODEL, CursorFields.SYSTEM_MODSTAMP, CUTOFF)
    )

    assert len(records) == 2
    assert records[0].Name == "record 1"
    assert records[0].Website == "https://one.example.com"


@pytest.mark.asyncio
async def test_rest_execute_drops_records_missing_chunks():
    http = FakeHTTP([
        _query_response([_rest_record(1), _rest_record(2)]),
        # The second query only returns the first record, as if the second record was
        # updated past the cutoff between the queries.
        _query_response([
            {"attributes": {"type": "Account"}, "Id": _id(1), "SystemModstamp": MODSTAMP, "Website": "https://one.example.com"},
        ]),
    ])
    manager = RestQueryManager(http, _LOG, INSTANCE_URL)

    records = await _collect(
        manager.execute("Account", CHUNK_QUERIES, CHUNKED_MODEL, CursorFields.SYSTEM_MODSTAMP, CUTOFF)
    )

    assert [r.Id for r in records] == [_id(1)]


@pytest.mark.asyncio
async def test_rest_execute_suppresses_records_updated_past_end():
    updated_past_cutoff = "2026-08-01T00:00:00.000Z"
    http = FakeHTTP([
        _query_response([_rest_record(1, modstamp=updated_past_cutoff)]),
    ])
    manager = RestQueryManager(http, _LOG, INSTANCE_URL)

    records = await _collect(
        manager.execute("Account", [CHUNK_QUERIES[0]], MODEL, CursorFields.SYSTEM_MODSTAMP, CUTOFF)
    )

    assert records == []
