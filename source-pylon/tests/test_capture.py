"""Offline tests; they never load credentials or call Pylon."""

import asyncio
import json
from collections.abc import AsyncGenerator, AsyncIterator, Callable
from datetime import UTC, datetime, timedelta
from logging import Logger, getLogger
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from freezegun import freeze_time
from estuary_cdk.capture.common import ResourceState
from estuary_cdk.http import HTTPSession
from pydantic import JsonValue, ValidationError

from source_pylon import api
from source_pylon.models import (
    Accounts,
    ApiKey,
    CustomFields,
    EndpointConfig,
    Issue,
    IssueContext,
    IssueMessage,
    PylonDocument,
    Teams,
    Users,
)
from source_pylon.resources import PylonResource, all_resources

LOG = getLogger(__name__)
NOW = datetime(2026, 9, 16, 12, 0, 10, 999000, tzinfo=UTC)
HORIZON = NOW.replace(microsecond=0) - api.LAG

Response = dict[str, Any] | Exception


class FakeHTTP:
    def __init__(self, *responses: Response):
        self.responses: list[Response] = list(responses)
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.streaming: bool = False

    async def request_stream(
        self, log: Logger, url: str, **kwargs: Any
    ) -> tuple[dict[str, str], Callable[[], AsyncGenerator[bytes, None]]]:
        assert not self.streaming, "parent response remained open during child request"
        self.calls.append((url, kwargs))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response

        async def body() -> AsyncGenerator[bytes, None]:
            self.streaming = True
            try:
                encoded = json.dumps(response).encode()
                for position in range(0, len(encoded), 13):
                    yield encoded[position : position + 13]
            finally:
                self.streaming = False

        return {}, body


@pytest.fixture(autouse=True)
def no_pacing(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(api.PylonClient, "_pace", AsyncMock())


def client(*responses: Response) -> tuple[api.PylonClient, FakeHTTP]:
    http = FakeHTTP(*responses)
    return api.PylonClient(cast(HTTPSession, http), api.API_URLS["US"]), http


def collect[T](iterator: AsyncIterator[T]) -> list[T]:
    async def drain() -> list[T]:
        return [item async for item in iterator]

    return asyncio.run(drain())


def issue(id: str, updated: datetime) -> dict[str, Any]:
    return {"id": id, "updated_at": updated.isoformat(), "title": "extra field"}


def config(**kwargs: Any) -> EndpointConfig:
    return EndpointConfig(credentials=ApiKey(access_token="offline"), **kwargs)


def issues_state(resources: list[PylonResource]) -> tuple[datetime, datetime]:
    state = next(r for r in resources if r.name == Issue.name).initial_state
    assert isinstance(state.inc, ResourceState.Incremental)
    assert isinstance(state.backfill, ResourceState.Backfill)
    assert isinstance(state.inc.cursor, datetime)
    assert isinstance(state.backfill.cutoff, datetime)
    return state.inc.cursor, state.backfill.cutoff


@pytest.mark.parametrize("model", [Accounts, Issue, IssueMessage])
def test_pages_follow_cursor(model: type[PylonDocument]):
    if model is Issue:
        first, second = issue("a", NOW), issue("b", NOW)
    else:
        first, second = {"id": "a"}, {"id": "b"}
    c, http = client(
        {"data": [first], "pagination": {"has_next_page": True, "cursor": "opaque+/="}},
        {"data": [second]},
    )
    kwargs: dict[str, Any] = {}
    if model is IssueMessage:
        kwargs = {
            "path": IssueMessage.path_for("parent"),
            "validation_context": IssueContext("parent"),
        }
    docs = collect(c.pages(LOG, model, **kwargs))
    assert [d.model_dump()["id"] for d in docs] == ["a", "b"]
    args = http.calls[1][1]["json" if model is Issue else "params"]
    assert args["cursor"] == "opaque+/="


@pytest.mark.parametrize(
    "pagination", [{"has_next_page": True, "cursor": ""}, {"has_next_page": True}]
)
def test_missing_next_cursor_fails(pagination: dict[str, JsonValue]):
    c, _ = client({"data": [], "pagination": pagination})
    with pytest.raises(RuntimeError, match="Missing"):
        collect(c.pages(LOG, Accounts))


def test_repeated_cursor_fails():
    response: dict[str, Any] = {
        "data": [{"id": "a"}],
        "pagination": {"has_next_page": True, "cursor": "same"},
    }
    c, http = client(response, response)
    with pytest.raises(RuntimeError, match="repeated"):
        collect(c.pages(LOG, Accounts))
    assert len(http.calls) == 2


@pytest.mark.parametrize("model", [Accounts, Issue, CustomFields])
def test_empty_response_may_omit_data(model: type[PylonDocument]):
    c, _ = client({"request_id": "empty"})
    assert collect(c.pages(LOG, model)) == []


def test_unpaginated_resource_rejects_next_page():
    c, _ = client({"data": [], "pagination": {"has_next_page": True, "cursor": "next"}})
    with pytest.raises(RuntimeError, match="Unexpected pagination"):
        collect(c.pages(LOG, Teams))


def test_users_include_deactivated():
    c, http = client({"data": [{"id": "former", "is_deactivated": True}]})
    docs = collect(api.snapshot_resources(c, Users, LOG))
    assert http.calls[0][1]["params"] == {"include_deactivated": "true"}
    assert docs[0].model_dump()["is_deactivated"] is True


def test_custom_fields_read_every_object_type():
    object_types = [p["object_type"] for p in CustomFields.partitions]
    c, http = client(
        *({"data": [{"id": kind, "object_type": kind}]} for kind in object_types)
    )
    docs = collect(api.snapshot_resources(c, CustomFields, LOG))
    assert [call[1]["params"]["object_type"] for call in http.calls] == object_types
    assert len(docs) == len(object_types)


def test_messages_read_every_issue_and_carry_issue_id():
    c, http = client(
        {
            "data": [issue("b", NOW)],
            "pagination": {"cursor": "next", "has_next_page": True},
        },
        {"data": [issue("a", NOW)]},
        {"data": [{"id": "m", "is_private": True}]},
        {"data": []},
    )
    docs = collect(api.snapshot_issue_messages(c, LOG))
    assert [(d.issue_id, d.model_dump()["is_private"]) for d in docs] == [("a", True)]
    assert [url.removeprefix(api.API_URLS["US"]) for url, _ in http.calls] == [
        "/issues/search",
        "/issues/search",
        "/issues/a/messages",
        "/issues/b/messages",
    ]
    assert "filter" not in http.calls[0][1]["json"]


def test_messages_require_issue_ids():
    c, http = client({"data": [issue("", NOW)]})
    with pytest.raises(ValidationError):
        collect(api.snapshot_issue_messages(c, LOG))
    assert len(http.calls) == 1


@freeze_time(NOW)
def test_incremental_emits_complete_window():
    cursor = HORIZON - api.MIN_WINDOW
    c, http = client(
        {
            "data": [issue("last", HORIZON), issue("margin", cursor)],
            "pagination": {"cursor": "next", "has_next_page": True},
        },
        {
            "data": [
                issue("first", cursor + api.TICK),
                issue("future", HORIZON + api.TICK),
            ]
        },
    )
    out = collect(api.fetch_issues(c, LOG, cursor))
    assert [d.id for d in out[:-1] if isinstance(d, Issue)] == ["last", "first"]
    assert out[-1] == HORIZON
    assert http.calls[0][1]["json"]["filter"]["values"] == [
        cursor.isoformat(),
        (HORIZON + api.TICK).isoformat(),
    ]


@freeze_time(NOW)
def test_incremental_waits_for_min_window():
    c, http = client()
    assert collect(api.fetch_issues(c, LOG, HORIZON - api.MIN_WINDOW + api.TICK)) == []
    assert not http.calls


def test_partial_window_never_checkpoints():
    cursor = NOW - timedelta(days=1)
    c, _ = client(
        {
            "data": [issue("a", cursor + api.TICK)],
            "pagination": {"cursor": "next", "has_next_page": True},
        },
        RuntimeError("network"),
    )
    output: list[object] = []

    async def run():
        async for item in api.fetch_issues(c, LOG, cursor):
            output.append(item)

    with pytest.raises(RuntimeError, match="network"):
        asyncio.run(run())
    assert output and all(isinstance(item, Issue) for item in output)


def test_backfill_windows_and_resume():
    start = datetime(2026, 9, 1, tzinfo=UTC)
    next_day = start + timedelta(days=1)
    cutoff = start + timedelta(days=2)
    c, _ = client({"data": [issue("first", start), issue("next", next_day)]})
    out = collect(api.backfill_issues(c, start, LOG, None, cutoff))
    assert [d.id for d in out if isinstance(d, Issue)] == ["first"]
    assert out[-1] == next_day.isoformat()
    c, _ = client(
        {"data": [issue("last", cutoff - api.TICK), issue("incremental", cutoff)]}
    )
    out = collect(api.backfill_issues(c, start, LOG, next_day.isoformat(), cutoff))
    assert [d.id for d in out if isinstance(d, Issue)] == ["last"]
    assert not any(isinstance(d, str) for d in out)


def test_fractional_future_start_date_is_floored():
    start = datetime(2099, 9, 1, 0, 0, 0, 500000, tzinfo=UTC)
    floored = start.replace(microsecond=0)
    cursor, cutoff = issues_state(
        asyncio.run(all_resources(LOG, AsyncMock(), config(start_date=start)))
    )
    assert cutoff == floored and cursor + api.TICK == floored
    c, http = client()
    assert collect(api.backfill_issues(c, floored, LOG, None, floored)) == []
    assert not http.calls


def test_discovery_makes_no_requests_and_seam_is_gapless():
    http = AsyncMock()
    resources = asyncio.run(all_resources(LOG, http, config()))
    http.request.assert_not_called()
    http.request_stream.assert_not_called()
    cursor, cutoff = issues_state(resources)
    assert cursor + api.TICK == cutoff and cutoff.microsecond == 0
