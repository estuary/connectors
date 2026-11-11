import asyncio
import base64
import json
import logging
import traceback
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiohttp import (
    ClientConnectorDNSError,
    ClientOSError,
    ClientPayloadError,
    ClientRequest,
    ClientResponseError,
    ConnectionTimeoutError,
    InvalidURL,
    RequestInfo,
    ServerDisconnectedError,
    TooManyRedirects,
)
from multidict import CIMultiDict, CIMultiDictProxy
from estuary_cdk.capture import common, request
from estuary_cdk.capture.common import ResourceConfigWithSchedule, ResourceState
from estuary_cdk.flow import CaptureBinding, ValidationError as ConnectorValidationError
from estuary_cdk.http import HTTPError
from pydantic import ValidationError
from yarl import URL

from source_twenty import Connector, api, resources
from source_twenty.models import (
    ApiKey,
    ConnectorState,
    EndpointConfig,
    ResourceConfig,
    TwentyRecord,
)

LOG = logging.getLogger(__name__)
BASE_URL = "https://twenty.example.com/"
NOW = datetime(2026, 11, 3, 17, 36, 21, 122987, tzinfo=UTC)
HORIZON = datetime(2026, 11, 3, 17, 35, 21, 122000, tzinfo=UTC)
CURSOR = datetime(2026, 11, 3, 17, 34, 0, 31000, tzinfo=UTC)
TICK = timedelta(milliseconds=1)


class FakeHTTP:
    """Queued JSON replies and recorded requests, without opening a connection."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []
        self.token_source = None
        self.completed_streams = 0

    async def request(self, log, url, params=None, headers=None, should_retry=None):
        # ClientRequest is aiohttp's real URL encoder; constructing it does no IO.
        request = ClientRequest("GET", URL(url), params=params, headers=headers)
        self.requests.append(
            {
                "url": request.url,
                "params": dict(params or {}),
                "headers": dict(headers or {}),
                "should_retry": should_retry,
            }
        )
        assert self.responses, "Unexpected HTTP request"
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        if isinstance(response, bytes):
            return response
        return json.dumps(response).encode()

    async def request_stream(
        self, log, url, params=None, headers=None, should_retry=None
    ):
        payload = await self.request(log, url, params, headers, should_retry)

        async def body():
            chunk_size = max(1, len(payload) // 3)
            for start in range(0, len(payload), chunk_size):
                yield payload[start : start + chunk_size]
            self.completed_streams += 1

        return {}, body


def patched_connector(monkeypatch, http):
    connector = Connector()
    monkeypatch.setattr(connector, "request", http.request)
    monkeypatch.setattr(connector, "request_stream", http.request_stream)
    return connector


def records_generator(http, backfill):
    if backfill:
        return api.backfill_records(http, BASE_URL, "people", LOG, None, HORIZON)
    return api.fetch_records(http, BASE_URL, "people", LOG, CURSOR)


def collect(generator):
    async def run():
        return [item async for item in generator]

    return asyncio.run(run())


def timestamp(value):
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def record(number, updated_at=CURSOR, **extra):
    return {
        "id": f"00000000-0000-0000-0000-{number:012d}",
        "createdAt": "2020-01-01T00:00:00.000Z",
        "updatedAt": timestamp(updated_at),
        "deletedAt": None,
        "searchVector": "'internal':1 'index':2",
        **extra,
    }


def response(data, next_cursor=None):
    return {
        "data": data,
        "pageInfo": {
            "hasNextPage": next_cursor is not None,
            "hasPreviousPage": False,
            "startCursor": "first",
            "endCursor": next_cursor or "last",
        },
        "totalCount": 0,
    }


def records_response(records, next_cursor=None, object_name="people"):
    return response({object_name: records}, next_cursor)


def encoded_cursor(data):
    return base64.urlsafe_b64encode(json.dumps(data).encode()).decode()


def object_metadata(name, **extra):
    return {
        "namePlural": name,
        "nameSingular": name.removesuffix("s"),
        "labelPlural": name.title(),
        "isActive": True,
        "isSystem": False,
        "isRemote": False,
        "fields": [],
        **extra,
    }


def config():
    return EndpointConfig(
        base_url=BASE_URL,
        credentials=ApiKey(access_token="unit-test-key"),
    )


def discover_request():
    return SimpleNamespace(config=config())


def validation_request(*names):
    return SimpleNamespace(
        config=config(),
        bindings=[
            request.ValidateBinding[ResourceConfig](
                resourceConfig=ResourceConfig(name=name)
            )
            for name in names
        ],
    )


def twenty_error(status, code, url=f"{BASE_URL}/rest/opportunities"):
    """An HTTPError shaped like the CDK's, with Twenty's error body (see bruno/_auth/bad key.yml)."""
    body = json.dumps(
        {
            "statusCode": status,
            "error": "Error",
            "messages": ["Entity performing the request does not have permission"],
            "code": code,
        }
    ).encode()
    return HTTPError(
        f"Encountered HTTP error status {status} which cannot be retried.\nURL: {url}\nResponse:\n{body.decode()}",
        status,
        body,
    )


@pytest.fixture(autouse=True)
def frozen_clock(monkeypatch):
    monkeypatch.setattr(api, "now", lambda: NOW)


def dns_error():
    return ClientConnectorDNSError(
        SimpleNamespace(host="twenty.example.com", port=443, ssl=True),
        OSError(8, "nodename nor servname provided"),
    )


def redirect_error():
    """An aiohttp error whose repr() holds the request headers, so the API key."""
    url = URL(f"{BASE_URL}rest/people")
    headers = CIMultiDictProxy(
        CIMultiDict({"Authorization": f"Bearer {config().credentials.access_token}"})
    )
    return TooManyRedirects(RequestInfo(url, "GET", headers, url), ())


class RoutedHTTP(FakeHTTP):
    """Replies by URL path, tracking how many requests are in flight at once."""

    def __init__(self, routes):
        super().__init__([])
        self.routes = routes
        self.active = self.peak = 0

    async def request(self, log, url, params=None, headers=None, should_retry=None):
        self.active += 1
        self.peak = max(self.peak, self.active)
        await asyncio.sleep(0.001)
        self.active -= 1
        self.responses = [self.routes[URL(url).path]]
        return await super().request(log, url, params, headers, should_retry)


@pytest.mark.parametrize(
    "code, message",
    [
        (401, "Invalid API key"),
        (403, "the Data Model settings permission"),
        (
            404,
            f"Incorrect base URL: Twenty's metadata endpoint was not found at {BASE_URL}",
        ),
        (500, "Encountered error validating API key"),
    ],
)
def test_validate_credentials_explains_http_errors(code, message):
    http = FakeHTTP([HTTPError("test failure", code)])

    with pytest.raises(ConnectorValidationError) as exc:
        asyncio.run(resources.validate_credentials(LOG, http, config()))

    assert len(exc.value.errors) == 1
    assert message in exc.value.errors[0]
    assert "test failure" in exc.value.errors[0]


@pytest.mark.parametrize("payload", [b"<html>Twenty web app</html>", {"data": []}])
def test_validate_credentials_rejects_non_api_responses(payload):
    http = FakeHTTP([payload])

    with pytest.raises(ConnectorValidationError) as exc:
        asyncio.run(resources.validate_credentials(LOG, http, config()))

    assert f"This doesn't look like a Twenty API at {BASE_URL}" in exc.value.errors[0]
    assert "First problem in the metadata response:" in exc.value.errors[0]


@pytest.mark.parametrize(
    "error, hint",
    [
        (dns_error(), "Couldn't connect to Twenty at"),
        (InvalidURL("not a url"), "Couldn't connect to Twenty at"),
        (ClientPayloadError("connection reset"), "failed. Please try again."),
        (TimeoutError("timed out"), "failed. Please try again."),
        (ConnectionResetError("reset by peer"), "failed. Please try again."),
    ],
)
def test_validate_credentials_explains_connection_errors(error, hint):
    http = FakeHTTP([error])

    with pytest.raises(ConnectorValidationError) as exc:
        asyncio.run(resources.validate_credentials(LOG, http, config()))

    assert hint in exc.value.errors[0]
    assert BASE_URL in exc.value.errors[0]
    # Only a connect or address error points at the base URL.
    assert ("confirm the base URL" in exc.value.errors[0]) == (
        hint.startswith("Couldn't connect")
    )


def test_connection_errors_never_show_the_api_key(monkeypatch):
    key = config().credentials.access_token
    assert key in repr(redirect_error())

    with pytest.raises(ConnectorValidationError) as exc:
        asyncio.run(
            resources.validate_credentials(LOG, FakeHTTP([redirect_error()]), config())
        )
    assert exc.value.errors[0].endswith("\n\nTooManyRedirects")
    assert key not in exc.value.errors[0]

    http = FakeHTTP(
        [
            response([object_metadata("people")]),
            response([object_metadata("people")]),
            redirect_error(),
        ]
    )
    connector = patched_connector(monkeypatch, http)
    with pytest.raises(ConnectorValidationError) as exc:
        asyncio.run(connector.validate(LOG, validation_request("people")))
    assert exc.value.errors == [
        "Object 'people': Connection failed while reading it. TooManyRedirects"
    ]


def test_validate_reports_a_bad_url_as_a_connection_failure(monkeypatch):
    # InvalidURL is also a ValueError; it must not read as an invalid API response.
    http = FakeHTTP(
        [
            response([object_metadata("people")]),
            response([object_metadata("people")]),
            InvalidURL("not a url"),
        ]
    )
    connector = patched_connector(monkeypatch, http)
    with pytest.raises(ConnectorValidationError) as exc:
        asyncio.run(connector.validate(LOG, validation_request("people")))
    assert exc.value.errors == [
        "Object 'people': Connection failed while reading it. InvalidURL"
    ]


def test_validate_credentials_accepts_metadata():
    http = FakeHTTP([response([object_metadata("people")])])

    asyncio.run(resources.validate_credentials(LOG, http, config()))

    assert http.token_source.credentials == config().credentials
    assert len(http.requests) == 1
    assert http.requests[0]["url"].path == "/rest/metadata/objects"
    assert http.requests[0]["params"] == {"limit": 1}
    assert http.requests[0]["headers"] == api.HEADERS


def test_validate_reports_a_broken_connection_per_object(monkeypatch):
    http = FakeHTTP(
        [
            response([object_metadata("people")]),
            response([object_metadata("companies"), object_metadata("people")]),
            ClientPayloadError("Response payload is not completed"),
            HTTPError("Forbidden", 403),
        ]
    )
    connector = patched_connector(monkeypatch, http)

    with pytest.raises(ConnectorValidationError) as exc:
        asyncio.run(connector.validate(LOG, validation_request("companies", "people")))

    assert exc.value.errors[0].startswith(
        "Object 'companies': Connection failed while reading it."
    )
    assert (
        exc.value.errors[1]
        == "Object 'people': The API key's role can't read it. Forbidden"
    )


@pytest.mark.parametrize("code", [400, 403])
def test_validate_explains_permission_denied(monkeypatch, code):
    http = FakeHTTP(
        [
            response([object_metadata("people")]),
            response([object_metadata("opportunities"), object_metadata("people")]),
            twenty_error(code, "PERMISSION_DENIED"),
            records_response([], object_name="people"),
        ]
    )
    connector = patched_connector(monkeypatch, http)

    with pytest.raises(ConnectorValidationError) as exc:
        asyncio.run(
            connector.validate(LOG, validation_request("opportunities", "people"))
        )

    assert exc.value.errors == [
        "Object 'opportunities': The API key's role can't read it. Give the role read access to this object (for workflows, the Workflows permission), or disable its binding."
    ]


def test_validate_collects_object_read_failures(monkeypatch):
    names = ["people", "companies", "customWidgets", "tasks"]
    http = FakeHTTP(
        [
            response([object_metadata("people")]),
            response([object_metadata(name) for name in names]),
            HTTPError("Forbidden", 403),
            HTTPError("Not found", 404),
            HTTPError("Read temporarily unavailable", 503),
            records_response([], object_name="tasks"),
        ]
    )
    connector = patched_connector(monkeypatch, http)

    with pytest.raises(ConnectorValidationError) as exc:
        asyncio.run(connector.validate(LOG, validation_request(*names)))

    assert exc.value.errors == [
        "Object 'people': The API key's role can't read it. Forbidden",
        "Object 'companies': HTTP 404: Not found",
        "Object 'customWidgets': HTTP 503: Read temporarily unavailable",
    ]
    assert [r["url"].path for r in http.requests[2:]] == [
        f"/rest/{name}" for name in names
    ]
    for read in http.requests[2:]:
        # The probe sends the capture's own filter and sort order.
        assert set(read["params"]) == {"limit", "depth", "order_by", "filter"}
        assert read["params"]["limit"] == 1
        assert read["params"]["order_by"] == TwentyRecord.ORDER_BY
        assert TwentyRecord.INCLUDE_DELETED_FILTER in read["params"]["filter"]
        assert read["headers"] == api.HEADERS
        assert read["should_retry"] is api.validation_should_retry
    assert http.requests[0]["should_retry"] is api.validation_should_retry
    assert not http.responses


def test_validate_passes_when_every_bound_object_is_readable(monkeypatch):
    http = FakeHTTP(
        [
            response([object_metadata("people")]),
            response([object_metadata("companies"), object_metadata("people")]),
            records_response([record(1)], "not-followed"),
            records_response([], object_name="companies"),
        ]
    )
    connector = patched_connector(monkeypatch, http)

    validated = asyncio.run(
        connector.validate(LOG, validation_request("people", "companies"))
    )

    assert [binding.resourcePath for binding in validated.bindings] == [
        ["people"],
        ["companies"],
    ]
    assert len(http.requests) == 4
    assert not http.responses


@pytest.mark.parametrize(
    "objects",
    [
        [],
        [object_metadata("customWidgets", isActive=False)],
        [object_metadata("renamedWidgets")],
    ],
    ids=["removed", "deactivated", "renamed"],
)
def test_validate_explains_missing_bound_objects(monkeypatch, objects):
    http = FakeHTTP([response(objects[:1]), response(objects)])
    connector = Connector()
    monkeypatch.setattr(connector, "request", http.request)

    with pytest.raises(ConnectorValidationError) as exc:
        asyncio.run(connector.validate(LOG, validation_request("customWidgets")))

    assert exc.value.errors == [
        "Object 'customWidgets': It was removed, deactivated or renamed in Twenty, or it is a system or remote object "
        "this connector doesn't capture. "
        "Re-discover the capture to update its bindings."
    ]
    # A missing object is reported from metadata alone; it isn't read.
    assert len(http.requests) == 2
    assert not http.responses


def test_validate_collects_missing_and_unreadable_objects(monkeypatch):
    http = FakeHTTP(
        [
            response([object_metadata("people")]),
            response([object_metadata("people")]),
            HTTPError("Forbidden", 403),
        ]
    )
    connector = patched_connector(monkeypatch, http)

    with pytest.raises(ConnectorValidationError) as exc:
        asyncio.run(
            connector.validate(LOG, validation_request("customWidgets", "people"))
        )

    assert exc.value.errors == [
        "Object 'customWidgets': It was removed, deactivated or renamed in Twenty, or it is a system or remote object "
        "this connector doesn't capture. "
        "Re-discover the capture to update its bindings.",
        "Object 'people': The API key's role can't read it. Forbidden",
    ]
    assert not http.responses


@pytest.mark.parametrize(
    "objects",
    [
        [],
        [object_metadata("customWidgets", isActive=False)],
        [object_metadata("renamedWidgets")],
    ],
    ids=["removed", "deactivated", "renamed"],
)
def test_open_preserves_binding_positions_and_skips_missing_objects(
    monkeypatch, caplog, objects
):
    http = FakeHTTP(
        [
            response([object_metadata("companies"), *objects], "metadata-next"),
            response([object_metadata("people"), object_metadata("unbound")]),
        ]
    )
    names = ["people", "customWidgets", "companies", "people"]
    bindings = [
        CaptureBinding[ResourceConfig](
            resourceConfig=ResourceConfig(name=name),
            resourcePath=[name],
            stateKey=f"{name}-{index}",
        )
        for index, name in enumerate(names)
    ]
    open_request = SimpleNamespace(
        capture=SimpleNamespace(config=config(), bindings=bindings),
        state=ConnectorState(),
    )
    task = SimpleNamespace(
        log=LOG,
        checkpoint=AsyncMock(),
        stopping=SimpleNamespace(webhook_task=None),
    )
    opened_bindings = []

    def open_binding(binding, binding_index, state, task, **kwargs):
        opened_bindings.append((binding_index, binding.resourceConfig.name))
        assert kwargs["fetch_page"].args[2] == binding.resourceConfig.name
        assert kwargs["fetch_changes"].args[2] == binding.resourceConfig.name
        assert state.inc.cursor == state.backfill.cutoff == HORIZON

    connector = Connector()
    monkeypatch.setattr(connector, "request", http.request)
    monkeypatch.setattr(resources.common, "open_binding", open_binding)

    async def run():
        _, capture = await connector.open(LOG, open_request)
        await capture(task)

    with caplog.at_level(logging.WARNING):
        asyncio.run(run())

    assert opened_bindings == [(0, "people"), (2, "companies"), (3, "people")]
    assert [
        list(call.args[0].bindingStateV1) for call in task.checkpoint.await_args_list
    ] == [[binding.stateKey] for binding in bindings]
    assert len(caplog.records) == 1
    warning = caplog.records[0]
    assert warning.levelno == logging.WARNING
    assert "no longer in the Twenty workspace" in warning.getMessage()
    assert "Remove its binding" in warning.getMessage()
    assert warning.args == {"object": "customWidgets"}
    assert len(http.requests) == 2
    assert http.requests[1]["params"]["starting_after"] == "metadata-next"
    # A running capture keeps the CDK's default retries for metadata.
    assert all(r["should_retry"] is None for r in http.requests)
    assert connector.token_source.credentials == config().credentials
    assert not http.responses


def test_discovery_filters_objects_and_registers_scheduled_resources():
    http = FakeHTTP(
        [
            response(
                [
                    object_metadata("people"),
                    object_metadata("customWidgets"),
                    object_metadata("inactive", isActive=False),
                    object_metadata("remote", isRemote=True),
                    object_metadata("hidden", isSystem=True),
                    object_metadata("noteTargets", isSystem=True),
                    object_metadata("taskTargets", isSystem=True),
                    object_metadata("noteTargets", isSystem=True, isActive=False),
                    object_metadata("taskTargets", isSystem=True, isRemote=True),
                ]
            )
        ]
    )

    discovered = asyncio.run(resources.all_resources(LOG, http, config()))

    # Sorted by name, so discovery output doesn't depend on Twenty's order.
    assert [r.name for r in discovered] == [
        "customWidgets",
        "noteTargets",
        "people",
        "taskTargets",
    ]
    for resource in discovered:
        assert resource.key == ["/id"]
        assert resource.model is TwentyRecord
        assert resource.schema_inference
        assert isinstance(resource.initial_config, ResourceConfigWithSchedule)
        assert resource.initial_config.name == resource.name
        assert resource.initial_config.interval == timedelta(minutes=5)
        assert resource.initial_config.schedule == ""
        state = resource.initial_state
        assert isinstance(state.inc, ResourceState.Incremental)
        assert isinstance(state.backfill, ResourceState.Backfill)
        assert state.inc.cursor == state.backfill.cutoff == HORIZON
        assert state.backfill.next_page is None

    schema = common.discovered(discovered).bindings[0].documentSchema
    assert "searchVector" not in schema["properties"]
    assert "searchVector" not in schema["required"]


def test_metadata_pagination_is_used_during_discovery():
    cursor = "metadata+/cursor=="
    http = FakeHTTP(
        [
            response([object_metadata("people")], cursor),
            response([object_metadata("customWidgets")]),
        ]
    )

    discovered = asyncio.run(resources.all_resources(LOG, http, config()))

    assert [r.name for r in discovered] == ["customWidgets", "people"]
    assert [r["params"] for r in http.requests] == [
        {"limit": 200},
        {"limit": 200, "starting_after": cursor},
    ]
    for sent in http.requests:
        assert sent["url"].path == "/rest/metadata/objects"
        assert sent["headers"] == api.HEADERS
        assert sent["should_retry"] is api.validation_should_retry
    assert http.requests[1]["url"].query["starting_after"] == cursor


def test_backfill_pages_and_resumes_an_opaque_cursor_with_timestamp_ties():
    page_cursor = "eyJ1cGRhdGVkQXQiOiIuMDMxMTIzWiJ9+/=="
    rows = [record(i, CURSOR - TICK) for i in range(201)]
    http = FakeHTTP(
        [
            records_response(rows[:200], page_cursor, "customWidgets"),
            records_response(rows[200:], object_name="customWidgets"),
        ]
    )

    first = collect(
        api.backfill_records(http, BASE_URL, "customWidgets", LOG, None, CURSOR)
    )

    assert [doc.id for doc in first[:-1]] == [row["id"] for row in rows[:200]]
    assert first[-1] == page_cursor
    assert len(http.requests) == 1

    resumed = collect(
        api.backfill_records(http, BASE_URL, "customWidgets", LOG, first[-1], CURSOR)
    )

    assert [doc.id for doc in resumed] == [rows[-1]["id"]]
    expected_filter = (
        'and(updatedAt[lt]:"2026-11-03T17:34:00.031Z",'
        "or(deletedAt[is]:NULL,deletedAt[is]:NOT_NULL))"
    )
    for sent in http.requests:
        assert sent["url"].path == "/rest/customWidgets"
        assert sent["headers"] == api.HEADERS
        assert sent["params"]["limit"] == 200
        assert sent["params"]["depth"] == 0
        assert sent["params"]["order_by"] == (
            "updatedAt[AscNullsFirst],id[AscNullsFirst]"
        )
        assert sent["params"]["filter"] == expected_filter
    assert "starting_after" not in http.requests[0]["params"]
    assert http.requests[1]["params"]["starting_after"] == page_cursor
    assert http.requests[1]["url"].query["starting_after"] == page_cursor


def test_empty_backfill_finishes_without_a_cursor():
    http = FakeHTTP([records_response([])])
    assert (
        collect(api.backfill_records(http, BASE_URL, "people", LOG, None, CURSOR)) == []
    )


def test_incremental_window_pagination_and_strictly_increasing_checkpoints():
    rows = [
        record(1),
        record(2, CURSOR + TICK),
        record(3, CURSOR + TICK),
        record(4, HORIZON - TICK),
    ]
    http = FakeHTTP(
        [
            records_response(rows[:1], "page-1"),
            records_response(rows[1:2], "page-2"),
            records_response(rows[2:3], "page-3"),
            records_response(rows[3:]),
        ]
    )

    items = collect(api.fetch_records(http, BASE_URL, "people", LOG, CURSOR))

    docs = [item for item in items if isinstance(item, TwentyRecord)]
    checkpoints = [item for item in items if isinstance(item, datetime)]
    assert [doc.id for doc in docs] == [row["id"] for row in rows]
    assert checkpoints == [CURSOR + TICK, HORIZON]
    assert items[2] == CURSOR + TICK
    assert items[-1] == HORIZON
    expected_filter = (
        'and(updatedAt[gte]:"2026-11-03T17:34:00.031Z",'
        'updatedAt[lt]:"2026-11-03T17:35:21.122Z",'
        "or(deletedAt[is]:NULL,deletedAt[is]:NOT_NULL))"
    )
    assert [r["params"].get("starting_after") for r in http.requests] == [
        None,
        "page-1",
        "page-2",
        "page-3",
    ]
    for index, sent in enumerate(http.requests):
        assert set(sent["params"]) == {"limit", "depth", "order_by", "filter"} | (
            {"starting_after"} if index else set()
        )
        assert sent["params"]["filter"] == expected_filter
        assert sent["params"]["limit"] == 200
        assert sent["params"]["depth"] == 0
        assert sent["params"]["order_by"] == (
            "updatedAt[AscNullsFirst],id[AscNullsFirst]"
        )
        assert sent["headers"] == api.HEADERS


def test_incremental_restart_replays_checkpoint_timestamp_without_losing_ties():
    rows = [record(i, CURSOR + TICK) for i in range(3)]
    http = FakeHTTP([records_response(rows[:1], "uncommitted-next-page")])

    async def stop_at_checkpoint():
        generator = api.fetch_records(http, BASE_URL, "people", LOG, CURSOR)
        first = await anext(generator)
        checkpoint = await anext(generator)
        await generator.aclose()
        return first, checkpoint

    first, checkpoint = asyncio.run(stop_at_checkpoint())
    assert first.id == rows[0]["id"]
    assert checkpoint == CURSOR + TICK

    resumed_http = FakeHTTP([records_response(rows)])
    resumed = collect(
        api.fetch_records(resumed_http, BASE_URL, "people", LOG, checkpoint)
    )

    assert [doc.id for doc in resumed[:-1]] == [row["id"] for row in rows]
    assert resumed[-1] == HORIZON
    assert "starting_after" not in resumed_http.requests[0]["params"]
    assert (
        'updatedAt[gte]:"2026-11-03T17:34:00.032Z"'
        in (resumed_http.requests[0]["params"]["filter"])
    )


@pytest.mark.parametrize("cursor", [HORIZON, HORIZON + TICK])
def test_incremental_returns_early_when_horizon_does_not_advance(cursor):
    http = FakeHTTP([])
    assert collect(api.fetch_records(http, BASE_URL, "people", LOG, cursor)) == []
    assert http.requests == []


def test_empty_incremental_window_does_not_advance_cursor():
    http = FakeHTTP([records_response([])])
    assert collect(api.fetch_records(http, BASE_URL, "people", LOG, CURSOR)) == []
    assert len(http.requests) == 1


def test_incremental_empty_final_page_still_checkpoints_emitted_documents():
    http = FakeHTTP([records_response([record(1)], "next"), records_response([])])
    items = collect(api.fetch_records(http, BASE_URL, "people", LOG, CURSOR))
    assert isinstance(items[0], TwentyRecord)
    assert items[1:] == [HORIZON]


@pytest.mark.parametrize("backfill", [True, False])
def test_delete_restore_and_excluded_search_vector(backfill):
    http = FakeHTTP(
        [
            records_response(
                [
                    record(1, deletedAt=timestamp(CURSOR)),
                    record(1, CURSOR + TICK, companyId=None, customField="kept"),
                ]
            )
        ]
    )
    generator = records_generator(http, backfill)
    docs = [item for item in collect(generator) if isinstance(item, TwentyRecord)]

    assert [doc.meta_.op for doc in docs] == ["d", "u"]
    for doc in docs:
        assert "searchVector" not in doc.model_dump()
        assert "searchVector" not in doc.model_dump_json()
    # The CDK emits documents with exclude_unset, so check that form too.
    emitted = [
        doc.model_dump(mode="json", by_alias=True, exclude_unset=True) for doc in docs
    ]
    assert emitted[0]["_meta"]["op"] == "d"
    assert emitted[1].get("_meta", {"op": "u"})["op"] == "u"
    restored = docs[-1].model_dump(mode="json", by_alias=True)
    assert restored["_meta"]["op"] == "u"
    assert restored["companyId"] is None
    assert restored["customField"] == "kept"
    assert restored["createdAt"] == "2020-01-01T00:00:00.000Z"


@pytest.mark.parametrize("backfill", [True, False], ids=["backfill", "incremental"])
def test_streamed_fractional_numbers_stay_numbers(backfill):
    http = FakeHTTP(
        [
            records_response(
                [
                    record(
                        1,
                        score=12.75,
                        count=3,
                        address={"addressLat": 52.52, "addressLng": -0.1},
                        raw={"values": [1, 2.5, "3.5"]},
                    )
                ]
            )
        ]
    )
    generator = records_generator(http, backfill)
    docs = [item for item in collect(generator) if isinstance(item, TwentyRecord)]

    emitted = json.loads(docs[0].model_dump_json(by_alias=True, exclude_unset=True))
    assert emitted["score"] == 12.75
    assert emitted["count"] == 3
    assert emitted["address"] == {"addressLat": 52.52, "addressLng": -0.1}
    assert emitted["raw"] == {"values": [1, 2.5, "3.5"]}


def test_resource_wiring_uses_the_same_millisecond_cutoff(monkeypatch):
    http = FakeHTTP(
        [
            response([object_metadata("customWidgets")]),
            records_response([record(1, HORIZON - TICK)], object_name="customWidgets"),
            records_response([record(2, HORIZON)], object_name="customWidgets"),
        ]
    )
    resource = asyncio.run(resources.all_resources(LOG, http, config()))[0]
    fetchers = {}

    def open_binding(binding, binding_index, state, task, **kwargs):
        fetchers.update(kwargs)

    monkeypatch.setattr(resources.common, "open_binding", open_binding)
    resource.open(None, 0, resource.initial_state, None, [])
    state = resource.initial_state
    backfill = collect(fetchers["fetch_page"](LOG, None, state.backfill.cutoff))
    monkeypatch.setattr(api, "now", lambda: NOW + timedelta(minutes=5))
    incremental = collect(fetchers["fetch_changes"](LOG, state.inc.cursor))

    assert [doc.id for doc in backfill] == [record(1)["id"]]
    assert incremental[0].id == record(2)["id"]
    assert incremental[-1] == HORIZON + timedelta(minutes=5)
    assert (
        'updatedAt[lt]:"2026-11-03T17:35:21.122Z"'
        in (http.requests[1]["params"]["filter"])
    )
    assert (
        'updatedAt[gte]:"2026-11-03T17:35:21.122Z"'
        in (http.requests[2]["params"]["filter"])
    )


@pytest.mark.parametrize("field", ["isActive", "isRemote", "isSystem", "namePlural"])
def test_discovery_requires_fields_used_to_select_streams(field):
    metadata = object_metadata("people")
    del metadata[field]
    http = FakeHTTP([response([metadata])])
    with pytest.raises(ValidationError):
        asyncio.run(resources.all_resources(LOG, http, config()))


@pytest.mark.parametrize("field", ["id", "updatedAt", "deletedAt"])
def test_record_contract_requires_key_cursor_and_deletion_status(field):
    payload = record(1)
    del payload[field]
    http = FakeHTTP([records_response([payload])])
    with pytest.raises(ValidationError):
        collect(api.fetch_records(http, BASE_URL, "people", LOG, CURSOR))


@pytest.mark.parametrize(
    "backfill, rows", [(False, []), (True, []), (True, [record(1)])]
)
def test_record_response_names_expected_and_present_keys(backfill, rows):
    http = FakeHTTP([records_response(rows, object_name="companies")])
    with pytest.raises(
        ValueError, match="Expected object key 'people'.*found keys: \\['companies'\\]"
    ):
        collect(records_generator(http, backfill))


def test_validate_reports_invalid_object_responses(monkeypatch):
    http = FakeHTTP(
        [
            response([object_metadata("people")]),
            response([object_metadata("people")]),
            records_response([], object_name="companies"),
        ]
    )
    connector = patched_connector(monkeypatch, http)

    with pytest.raises(ConnectorValidationError) as exc:
        asyncio.run(connector.validate(LOG, validation_request("people")))

    assert len(exc.value.errors) == 1
    assert "Object 'people': Invalid Twenty API response" in exc.value.errors[0]
    assert "Expected object key 'people'" in exc.value.errors[0]
    assert "found keys: ['companies']" in exc.value.errors[0]


@pytest.mark.parametrize(
    "payload", [b"<html>Twenty web app</html>", b'{"data":{"people":[']
)
def test_validate_reports_malformed_record_json(monkeypatch, payload):
    http = FakeHTTP(
        [
            response([object_metadata("people")]),
            response([object_metadata("people")]),
            payload,
        ]
    )
    connector = patched_connector(monkeypatch, http)

    with pytest.raises(ConnectorValidationError) as exc:
        asyncio.run(connector.validate(LOG, validation_request("people")))

    assert len(exc.value.errors) == 1
    assert "Object 'people': Invalid Twenty API response" in exc.value.errors[0]


@pytest.mark.parametrize("metadata", [True, False])
def test_a_next_page_requires_its_cursor(metadata):
    payload = response([]) if metadata else records_response([record(1)])
    payload["pageInfo"].update(hasNextPage=True, endCursor=None)
    http = FakeHTTP([payload])
    with pytest.raises(ValidationError, match="endCursor"):
        if metadata:
            collect(api.list_objects(LOG, http, BASE_URL))
        else:
            collect(api.backfill_records(http, BASE_URL, "people", LOG, None, HORIZON))


def test_validation_reads_stop_retrying_server_errors():
    # The CDK retries 5xx forever unless should_retry says no; validation must finish with an error.
    assert [
        api.validation_should_retry(503, None, b"", attempt) for attempt in (1, 2, 3, 4)
    ] == [True, True, False, False]


def test_discover_reports_a_bad_key_before_reading_objects(monkeypatch):
    http = FakeHTTP([HTTPError("Token invalid.", 401)])
    connector = Connector()
    monkeypatch.setattr(connector, "request", http.request)

    with pytest.raises(ConnectorValidationError) as exc:
        asyncio.run(connector.discover(LOG, discover_request()))

    assert exc.value.errors[0].startswith("Invalid API key.")
    assert len(http.requests) == 1


@pytest.mark.parametrize("status", [400, 403])
def test_discover_disables_objects_the_role_cannot_read(monkeypatch, caplog, status):
    http = FakeHTTP(
        [
            response([object_metadata("people")]),
            response([object_metadata("opportunities"), object_metadata("people")]),
            twenty_error(status, "PERMISSION_DENIED"),
            records_response([], object_name="people"),
        ]
    )
    connector = patched_connector(monkeypatch, http)

    with caplog.at_level(logging.WARNING):
        discovered = asyncio.run(connector.discover(LOG, discover_request()))

    # Left out, an existing binding would be removed when the capture is re-discovered.
    assert [(b.resourceConfig.name, b.disable) for b in discovered.bindings] == [
        ("opportunities", True),
        ("people", False),
    ]
    warnings = [r for r in caplog.records if "discovered disabled" in r.getMessage()]
    assert [w.getMessage() for w in warnings] == [
        "The API key's role can't read this object. It is discovered disabled."
    ]
    assert not http.responses


@pytest.mark.parametrize(
    "error, reason",
    [
        (HTTPError("Not found", 404), "failed with HTTP 404: Not found"),
        (twenty_error(400, "BAD_REQUEST"), "failed with HTTP 400: "),
        # The code only counts in Twenty's error body, not in the URL or message text.
        (
            twenty_error(400, "BAD_REQUEST", url=f"{BASE_URL}/rest/PERMISSION_DENIED"),
            "failed with HTTP 400: ",
        ),
        (
            HTTPError("PERMISSION_DENIED", 400, b"<html>proxy error</html>"),
            "failed with HTTP 400: PERMISSION_DENIED",
        ),
        (b"<html>proxy error</html>", "invalid Twenty API response"),
    ],
)
def test_discover_disables_an_object_whose_read_fails(
    monkeypatch, caplog, error, reason
):
    # One broken object must not fail discovery of the rest of the workspace.
    http = FakeHTTP(
        [
            response([object_metadata("people")]),
            response([object_metadata("opportunities"), object_metadata("people")]),
            error,
            records_response([], object_name="people"),
        ]
    )
    connector = patched_connector(monkeypatch, http)

    with caplog.at_level(logging.WARNING):
        discovered = asyncio.run(connector.discover(LOG, discover_request()))

    assert [(b.resourceConfig.name, b.disable) for b in discovered.bindings] == [
        ("opportunities", True),
        ("people", False),
    ]
    warnings = [r for r in caplog.records if "discovered disabled" in r.getMessage()]
    assert len(warnings) == 1
    assert reason in warnings[0].getMessage()
    assert warnings[0].args == {"object": "opportunities"}
    assert not http.responses


@pytest.mark.parametrize(
    "error",
    [
        HTTPError("Unauthorized", 401),
        twenty_error(500, "PERMISSION_DENIED"),
        HTTPError("Unavailable", 503),
        ClientPayloadError("connection reset"),
        ConnectionResetError("reset by peer"),
        TimeoutError("timed out"),
        InvalidURL("not a url"),
    ],
)
def test_discover_fails_on_errors_that_are_not_about_one_object(monkeypatch, error):
    # Disabling here would turn a bad key or an outage into a workspace of disabled bindings.
    http = RoutedHTTP(
        {
            "/rest/metadata/objects": response(
                [object_metadata("opportunities"), object_metadata("people")]
            ),
            "/rest/opportunities": error,
            "/rest/people": records_response([], object_name="people"),
        }
    )
    connector = patched_connector(monkeypatch, http)

    with pytest.raises(type(error)) as exc:
        asyncio.run(connector.discover(LOG, discover_request()))

    assert exc.value is error


def test_discover_fails_when_every_object_fails(monkeypatch):
    http = RoutedHTTP(
        {
            "/rest/metadata/objects": response(
                [object_metadata("opportunities"), object_metadata("people")]
            ),
            "/rest/opportunities": HTTPError("Bad request", 400),
            "/rest/people": HTTPError("Not found", 404),
        }
    )
    connector = patched_connector(monkeypatch, http)

    with pytest.raises(HTTPError, match="Bad request"):
        asyncio.run(connector.discover(LOG, discover_request()))


def test_discover_disables_every_object_a_role_cannot_read(monkeypatch):
    http = RoutedHTTP(
        {
            "/rest/metadata/objects": response(
                [object_metadata("opportunities"), object_metadata("people")]
            ),
            "/rest/opportunities": twenty_error(400, "PERMISSION_DENIED"),
            "/rest/people": twenty_error(400, "PERMISSION_DENIED"),
        }
    )
    connector = patched_connector(monkeypatch, http)

    discovered = asyncio.run(connector.discover(LOG, discover_request()))

    assert [b.disable for b in discovered.bindings] == [True, True]


@pytest.mark.parametrize("step", ["discover", "validate"])
def test_object_probes_run_concurrently_up_to_the_limit(monkeypatch, step):
    names = [f"object{i}" for i in range(resources.PROBE_CONCURRENCY + 2)]
    http = RoutedHTTP(
        {
            "/rest/metadata/objects": response([object_metadata(n) for n in names]),
            **{f"/rest/{n}": records_response([], object_name=n) for n in names},
        }
    )
    connector = patched_connector(monkeypatch, http)

    if step == "discover":
        asyncio.run(connector.discover(LOG, discover_request()))
    else:
        asyncio.run(connector.validate(LOG, validation_request(*names)))

    probed = [
        r["url"].path
        for r in http.requests
        if r["url"].path != "/rest/metadata/objects"
    ]
    assert sorted(probed) == sorted(f"/rest/{n}" for n in names)
    assert http.peak == resources.PROBE_CONCURRENCY


def test_bounded_probes_keep_input_order():
    async def probe(name):
        # Later names finish first.
        await asyncio.sleep(0.001 * (10 - int(name)))
        return name

    names = [str(i) for i in range(10)]
    assert asyncio.run(resources._gather_bounded(names, probe)) == names


def test_a_failing_probe_raises_itself_and_cancels_the_rest():
    cancelled = []

    async def probe(name):
        if name == "bad":
            raise ValueError("boom")
        try:
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            cancelled.append(name)
            raise

    async def run():
        with pytest.raises(ValueError, match="boom"):
            await resources._gather_bounded(["a", "bad", "b"], probe)
        # The other probes are stopped before the error comes back, not left running.
        return sorted(cancelled)

    assert asyncio.run(run()) == ["a", "b"]


@pytest.mark.parametrize(
    "pages",
    [
        [("people", "same-cursor")] * 2,
        [("people", "A"), ("companies", "B"), ("notes", "A")],
    ],
    ids=["repeat", "cycle"],
)
def test_metadata_paging_stops_on_a_cursor_cycle(pages):
    http = FakeHTTP(
        [response([object_metadata(name)], cursor) for name, cursor in pages]
    )
    with pytest.raises(RuntimeError, match="did not advance"):
        collect(api.list_objects(LOG, http, BASE_URL))


@pytest.mark.parametrize("padding", [True, False])
@pytest.mark.parametrize("suffix", ["Z", "+00"])
def test_backfill_rejects_a_cursor_cycle_across_calls(padding, suffix):
    cursors = [
        encoded_cursor(
            {"updatedAt": f"2026-11-03T09:12:44.50047{i}{suffix}", "id": "a"}
        )
        for i in (4, 5)
    ]
    if not padding:
        cursors = [cursor.rstrip("=") for cursor in cursors]
    first, second = cursors
    http = FakeHTTP(
        [
            records_response([record(i)], cursor)
            for i, cursor in enumerate([first, second, first])
        ]
    )

    page = None
    for expected in (first, second):
        items = collect(
            api.backfill_records(http, BASE_URL, "people", LOG, page, HORIZON)
        )
        assert items[-1] == expected
        page = items[-1]

    with pytest.raises(RuntimeError, match="did not advance") as exc:
        collect(api.backfill_records(http, BASE_URL, "people", LOG, page, HORIZON))

    assert first in str(exc.value)
    assert second in str(exc.value)
    assert [r["params"].get("starting_after") for r in http.requests] == [
        None,
        first,
        second,
    ]


@pytest.mark.parametrize(
    "current_time, current_id, next_time, next_id",
    [
        ("2026-11-03T09:12:44.500474Z", "z", "2026-11-03T09:12:44.500475Z", "a"),
        ("2026-11-03T09:12:44.500474Z", "a", "2026-11-03T09:12:44.500474Z", "b"),
        ("2026-11-03T10:12:44.500474+01:00", "z", "2026-11-03T09:12:44.500475Z", "a"),
        # A server that rounds the cursor's timestamp could break ties this way.
        ("2026-11-03T09:12:44.500474Z", "b", "2026-11-03T09:12:44.500474Z", "a"),
    ],
    ids=["microseconds", "id_tie_break", "timezone_offsets", "lower_id_tie_break"],
)
def test_backfill_accepts_advancing_cursor_keys(
    current_time, current_id, next_time, next_id
):
    current = encoded_cursor({"updatedAt": current_time, "id": current_id})
    following = encoded_cursor({"updatedAt": next_time, "id": next_id})
    http = FakeHTTP([records_response([record(1)], following)])

    items = collect(
        api.backfill_records(http, BASE_URL, "people", LOG, current, HORIZON)
    )

    assert items[0].id == record(1)["id"]
    assert items[1:] == [following]


@pytest.mark.parametrize(
    "current, following",
    [
        ("same", "same"),
        (
            encoded_cursor({"updatedAt": "2026-11-03T09:12:44.500474Z", "id": "a"}),
            encoded_cursor(
                {"updatedAt": "2026-11-03T10:12:44.500474+01:00", "id": "a"}
            ),
        ),
        (
            encoded_cursor({"updatedAt": "2026-11-03T09:12:44.500475Z", "id": "a"}),
            encoded_cursor(
                {"updatedAt": "2026-11-03T10:12:44.500474+01:00", "id": "z"}
            ),
        ),
        (
            base64.b64encode(
                b'{"updatedAt": "2026-11-03T09:12:44.500474Z", "id": "~~~???"}'
            ).decode(),
            encoded_cursor(
                {"updatedAt": "2026-11-03T09:12:44.500474Z", "id": "~~~???"}
            ),
        ),
    ],
    ids=["repeat", "equal_keys", "decreasing_microseconds", "encodings"],
)
def test_backfill_rejects_non_advancing_cursor_keys(current, following):
    if current != "same":
        # Distinct strings exercise decoded comparisons, including both encodings.
        assert current != following
    http = FakeHTTP([records_response([record(1)], following)])

    with pytest.raises(RuntimeError, match="did not advance") as exc:
        collect(api.backfill_records(http, BASE_URL, "people", LOG, current, HORIZON))

    assert current in str(exc.value)
    assert following in str(exc.value)


def test_decode_cursor_reads_twentys_live_format():
    # Page 1's endCursor and page 2's startCursor from bruno/Records/backfill page.yml.
    end = "eyJ1cGRhdGVkQXQiOiIyMDI2LTEwLTI5IDA3OjQ4OjMzLjQxMzg5OCswMCIsImlkIjoiNzY0YzVjOTktMTU0Ni00OWQxLWFkMWEtZDVlZmQxOGM5NDQ3In0"
    start = "eyJ1cGRhdGVkQXQiOiIyMDI2LTEwLTI5IDA3OjQ4OjMzLjQyMjA5KzAwIiwiaWQiOiI4NGY2OWIzYy02OWM0LTQ3NzItYTBjNy01Y2U0ZWU0NWQwNTAifQ"

    assert api._decode_cursor(end) == (
        datetime(2026, 10, 29, 7, 48, 33, 413898, tzinfo=UTC),
        "764c5c99-1546-49d1-ad1a-d5efd18c9447",
    )
    assert api._decode_cursor(start) == (
        datetime(2026, 10, 29, 7, 48, 33, 422090, tzinfo=UTC),
        "84f69b3c-69c4-4772-a0c7-5ce4ee45d050",
    )


@pytest.mark.parametrize(
    "opaque",
    [
        "not%base64",
        "opaque-é",
        base64.b64encode(b"not JSON").decode(),
        encoded_cursor({"id": "a"}),
        encoded_cursor({"updatedAt": "2026-11-03T09:12:44.500474Z"}),
        encoded_cursor({"updatedAt": "not a timestamp", "id": "a"}),
        encoded_cursor({"updatedAt": "2026-11-03T09:12:44.500474", "id": "a"}),
    ],
    ids=[
        "not_base64",
        "non_ascii",
        "not_json",
        "missing_time",
        "missing_id",
        "invalid_time",
        "naive_time",
    ],
)
def test_backfill_falls_back_to_repeat_check_for_unknown_cursor_formats(opaque):
    known = encoded_cursor({"updatedAt": "2026-11-03T09:12:44.500474Z", "id": "a"})
    http = FakeHTTP(
        [
            records_response([record(1)], known),
            records_response([record(2)], opaque),
            records_response([record(3)], opaque),
        ]
    )

    for current, following in ((opaque, known), (known, opaque)):
        items = collect(
            api.backfill_records(http, BASE_URL, "people", LOG, current, HORIZON)
        )
        assert isinstance(items[0], TwentyRecord)
        assert items[1:] == [following]

    with pytest.raises(RuntimeError, match="did not advance"):
        collect(api.backfill_records(http, BASE_URL, "people", LOG, opaque, HORIZON))


@pytest.mark.parametrize("backfill", [True, False])
def test_records_stream_before_page_completion_and_cursors_follow(backfill):
    # Exceed the processor's input buffer so the first record arrives before EOF.
    rows = [
        record(1, CURSOR + TICK),
        record(2, CURSOR + 2 * TICK, customField="x" * 200_000),
    ]
    http = FakeHTTP([records_response(rows, "next")])

    async def run():
        generator = records_generator(http, backfill)
        try:
            first = await anext(generator)
            assert first.id == rows[0]["id"]
            assert http.completed_streams == 0
            second = await anext(generator)
            assert second.id == rows[1]["id"]
            checkpoint = await anext(generator)
            assert checkpoint == ("next" if backfill else CURSOR + 2 * TICK)
            assert http.completed_streams == 1
            assert len(http.requests) == 1
        finally:
            await generator.aclose()

    asyncio.run(run())


def test_incremental_rejects_a_repeated_cursor():
    http = FakeHTTP(
        [
            records_response([record(1)], "A"),
            records_response([record(2)], "A"),
        ]
    )
    with pytest.raises(RuntimeError, match="did not advance"):
        collect(api.fetch_records(http, BASE_URL, "people", LOG, CURSOR))
    assert len(http.requests) == 2


def test_open_fails_when_every_bound_object_is_gone():
    http = FakeHTTP([response([object_metadata("companies")])])
    bindings = [
        CaptureBinding[ResourceConfig](
            resourceConfig=ResourceConfig(name="customWidgets"),
            resourcePath=["customWidgets"],
            stateKey="customWidgets",
        )
    ]
    with pytest.raises(RuntimeError, match="None of the capture's bound objects"):
        asyncio.run(resources.enabled_resources(LOG, http, config(), bindings))


@pytest.mark.parametrize(
    "companies, people, expected",
    [
        (response({"records": []}), response({"records": []}), ValueError),
        (
            twenty_error(400, "BAD_REQUEST"),
            twenty_error(400, "BAD_REQUEST"),
            HTTPError,
        ),
        (response({"records": []}), twenty_error(404, "NOT_FOUND"), ValueError),
    ],
    ids=["invalid-response", "400", "mixed"],
)
def test_discover_fails_when_no_object_is_readable_even_with_one_permission_denied(
    monkeypatch, companies, people, expected
):
    # A key without the Workflows permission is refused workflows in most workspaces,
    # so one PERMISSION_DENIED must not switch off the check for a workspace problem.
    http = RoutedHTTP(
        {
            "/rest/metadata/objects": response(
                [
                    object_metadata("companies"),
                    object_metadata("people"),
                    object_metadata("workflows"),
                ]
            ),
            "/rest/companies": companies,
            "/rest/people": people,
            "/rest/workflows": twenty_error(400, "PERMISSION_DENIED"),
        }
    )
    connector = patched_connector(monkeypatch, http)

    with pytest.raises(expected) as exc:
        asyncio.run(connector.discover(LOG, discover_request()))
    # The first failure that isn't the permission denial, in object order.
    assert type(exc.value) is expected
    assert "PERMISSION_DENIED" not in str(exc.value)


def test_discover_with_no_objects_returns_no_bindings(monkeypatch):
    http = RoutedHTTP({"/rest/metadata/objects": response([])})
    connector = patched_connector(monkeypatch, http)

    assert asyncio.run(connector.discover(LOG, discover_request())).bindings == []


def key_request_info(path="rest/metadata/objects"):
    url = URL(f"{BASE_URL}{path}")
    headers = CIMultiDictProxy(
        CIMultiDict({"Authorization": f"Bearer {config().credentials.access_token}"})
    )
    return RequestInfo(url, "GET", headers, url)


def test_an_unparseable_reply_is_not_reported_as_an_http_status():
    # aiohttp reports a reply it can't parse as a ClientResponseError with its
    # parser's code 400; Twenty never sent that status.
    error = ClientResponseError(
        key_request_info(),
        (),
        status=400,
        message="Bad status line:\n  Expected HTTP/:\n\n  b'SSH-2.0-OpenSSH_9.6'\n  ^",
    )

    with pytest.raises(ConnectorValidationError) as exc:
        asyncio.run(resources.validate_credentials(LOG, FakeHTTP([error]), config()))

    assert "HTTP 400" not in exc.value.errors[0]
    assert "SSH" not in exc.value.errors[0]
    assert config().credentials.access_token not in exc.value.errors[0]


def test_a_connect_timeout_points_at_the_base_url():
    # What aiohttp raises when a wrong host or port never answers.
    error = ConnectionTimeoutError(
        f"Connection timeout to host {BASE_URL}rest/metadata/objects?limit=1"
    )

    with pytest.raises(ConnectorValidationError) as exc:
        asyncio.run(resources.validate_credentials(LOG, FakeHTTP([error]), config()))

    assert "confirm the base URL" in exc.value.errors[0]


@pytest.mark.parametrize(
    "error",
    [ServerDisconnectedError(), ClientOSError(104, "Connection reset by peer")],
    ids=["server-disconnected", "connection-reset"],
)
def test_a_dropped_connection_does_not_blame_the_base_url(error):
    with pytest.raises(ConnectorValidationError) as exc:
        asyncio.run(resources.validate_credentials(LOG, FakeHTTP([error]), config()))

    assert "confirm the base URL" not in exc.value.errors[0]
    assert "Please try again" in exc.value.errors[0]
    assert exc.value.errors[0].endswith(type(error).__name__)


def discover_failure_log(monkeypatch, error):
    """Runs discover with one probe failing and returns what the CDK would log."""
    http = RoutedHTTP(
        {
            "/rest/metadata/objects": response(
                [object_metadata("opportunities"), object_metadata("people")]
            ),
            "/rest/opportunities": error,
            "/rest/people": records_response([], object_name="people"),
        }
    )
    connector = patched_connector(monkeypatch, http)
    with pytest.raises(type(error)) as exc:
        asyncio.run(connector.discover(LOG, discover_request()))
    # The CDK logs an unhandled error with traceback.format_exception, which also
    # prints the error's context.
    return "".join(traceback.format_exception(exc.value))


def test_a_failing_probe_is_logged_without_the_task_group(monkeypatch):
    log = discover_failure_log(monkeypatch, twenty_error(401, "UNAUTHENTICATED"))

    assert "ExceptionGroup" not in log


def echoed_key_error(status=403):
    """An error body that repeats the Authorization header, as some proxies do."""
    body = json.dumps(
        {
            "message": "Authorization header requires 'Credential' parameter. "
            f"Authorization=Bearer {config().credentials.access_token}"
        }
    ).encode()
    return HTTPError(
        f"Encountered HTTP error status {status} which cannot be retried.\nURL: {BASE_URL}rest/people\nResponse:\n{body.decode()}",
        status,
        body,
    )


@pytest.mark.parametrize("step", ["credentials", "validate", "discover"])
def test_error_messages_never_show_a_key_the_server_echoes(monkeypatch, caplog, step):
    key = config().credentials.access_token
    metadata = response([object_metadata("opportunities"), object_metadata("people")])

    if step == "credentials":
        with pytest.raises(ConnectorValidationError) as exc:
            asyncio.run(
                resources.validate_credentials(
                    LOG, FakeHTTP([echoed_key_error()]), config()
                )
            )
        shown = exc.value.errors
    else:
        http = RoutedHTTP(
            {
                "/rest/metadata/objects": metadata,
                "/rest/opportunities": echoed_key_error(),
                "/rest/people": records_response([], object_name="people"),
            }
        )
        connector = patched_connector(monkeypatch, http)
        if step == "validate":
            with pytest.raises(ConnectorValidationError) as exc:
                asyncio.run(
                    connector.validate(
                        LOG, validation_request("opportunities", "people")
                    )
                )
            shown = exc.value.errors
        else:
            with caplog.at_level(logging.WARNING):
                asyncio.run(connector.discover(LOG, discover_request()))
            shown = [r.getMessage() for r in caplog.records]

    assert shown
    assert not [message for message in shown if key in message]


@pytest.mark.parametrize("status", [401, 400, 500])
def test_a_discover_failure_never_shows_a_key_the_server_echoes(monkeypatch, status):
    # 401 and 500 fail discovery outright; a 400 on every object fails it as a
    # workspace problem.
    http = RoutedHTTP(
        {
            "/rest/metadata/objects": response([object_metadata("people")]),
            "/rest/people": echoed_key_error(status),
        }
    )
    connector = patched_connector(monkeypatch, http)

    with pytest.raises(HTTPError) as exc:
        asyncio.run(connector.discover(LOG, discover_request()))
    shown = "".join(traceback.format_exception(exc.value))
    assert exc.value.code == status
    assert config().credentials.access_token not in shown
    assert "<redacted>" in exc.value.message


@pytest.mark.parametrize("status", [401, 403, 404, 400])
def test_a_validation_error_chain_never_shows_a_key_the_server_echoes(status):
    with pytest.raises(ConnectorValidationError) as exc:
        asyncio.run(
            resources.validate_credentials(
                LOG, FakeHTTP([echoed_key_error(status)]), config()
            )
        )

    # The CDK logs the error with traceback.format_exception, which also prints
    # its cause and context.
    shown = "".join(traceback.format_exception(exc.value))
    assert "<redacted>" in shown
    assert config().credentials.access_token not in shown
