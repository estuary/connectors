import base64
import json
import sys
from datetime import UTC, datetime
from unittest.mock import Mock

import pytest
from azure.cosmos._change_feed import change_feed_start_from
from estuary_cdk.capture import request
from estuary_cdk.flow import CaptureBinding, CaptureSpec, CollectionSpec
from estuary_cdk.utils import json_merge_patch

from source_cosmosdb_nosql import Connector, api
from source_cosmosdb_nosql.models import ChangeFeedContinuation, ConnectorState, EndpointConfig, ResourceConfig

from .fakes import CONFIG, FakeTransport, capture_once, feed_headers, install_cosmos, sample_replies
from .test_capture import ITEMS, LOG, ORDERS, ORDERS_SCHEMA, TWO_RANGES, VALUE, caught_up, initialization, page


@pytest.fixture
def timestamp_probe():
    api.sdk_reads_point_in_time_as_seconds.cache_clear()
    yield
    api.sdk_reads_point_in_time_as_seconds.cache_clear()


@pytest.mark.parametrize("start_time", [
    "Beginning", datetime(1970, 1, 2, tzinfo=UTC), datetime(2026, 11, 11, 2, 5, 6, 123000, tzinfo=UTC),
])
@pytest.mark.parametrize("etag", [None, "", '"3"'])
@pytest.mark.parametrize("fixed_reader", [False, True])
def test_continuation_adapts_only_buggy_reader(monkeypatch, timestamp_probe, start_time, etag, fixed_reader):
    point_in_time = change_feed_start_from.ChangeFeedStartFromPointInTime

    def from_json(cls, data):
        value = data["PointInTimeMs"] / 1000 if fixed_reader else data["PointInTimeMs"]
        return cls(datetime.fromtimestamp(value, UTC))

    monkeypatch.setattr(point_in_time, "from_json", classmethod(from_json))
    start = change_feed_start_from.ChangeFeedStartFromInternal.from_start_time(start_time).to_dict()
    token = {
        "v": "v2", "containerRid": ORDERS["_rid"], "mode": "LatestVersion", "startFrom": start,
        "continuation": {
            "range": {"min": "", "max": "FF"},
            "continuation": [
                {"token": etag, "range": {"min": "", "max": "80"}},
                {"token": '"7"', "range": {"min": "80", "max": "FF"}},
            ],
        },
    }
    continuation = base64.b64encode(json.dumps(token).encode()).decode()
    adapted = api.continuation_for_sdk(continuation)
    if (start_time == "Beginning" or fixed_reader) and etag is not None:
        assert adapted == continuation
    assert json.loads(base64.b64decode(continuation)) == token
    if start_time != "Beginning" and not fixed_reader:
        token["startFrom"] = {**start, "PointInTimeMs": start["PointInTimeMs"] / 1000}
    token["continuation"]["continuation"][0]["token"] = etag or ""
    assert json.loads(base64.b64decode(adapted)) == token
    assert change_feed_start_from.ChangeFeedStartFromInternal.from_json(token["startFrom"]).to_dict() == start


def test_timestamp_probe_runs_once_at_startup(monkeypatch, timestamp_probe):
    start_from = change_feed_start_from.ChangeFeedStartFromInternal
    serialize = Mock(wraps=start_from.from_start_time)
    monkeypatch.setattr(start_from, "from_start_time", serialize)
    Connector()
    Connector()
    api.sdk_reads_point_in_time_as_seconds()
    assert serialize.call_count == 1


@pytest.mark.parametrize("changed", ["module", "class", "writer", "reader", "timestamp"])
def test_changed_sdk_shape_fails_at_startup(monkeypatch, timestamp_probe, changed):
    start_from = change_feed_start_from.ChangeFeedStartFromInternal
    point_in_time = change_feed_start_from.ChangeFeedStartFromPointInTime
    if changed == "module":
        monkeypatch.setitem(sys.modules, change_feed_start_from.__name__, None)
    elif changed == "class":
        monkeypatch.delattr(change_feed_start_from, "ChangeFeedStartFromInternal")
    elif changed == "writer":
        monkeypatch.setattr(point_in_time, "to_dict", lambda self: {"Type": "PointInTime", "seconds": 1})
    elif changed == "reader":
        monkeypatch.setattr(start_from, "from_json", lambda data: None)
    else:
        monkeypatch.setattr(start_from, "from_json", lambda data: point_in_time(datetime(2000, 1, 1, tzinfo=UTC)))
    with pytest.raises(RuntimeError, match="Cannot verify the installed azure-cosmos SDK's change feed timestamp serialization"):
        Connector()


def capture_spec(advanced):
    return CaptureSpec[EndpointConfig, ResourceConfig](
        name="test/capture", connectorType="IMAGE", intervalSeconds=0,
        config=EndpointConfig(**CONFIG, advanced={"disable_endpoint_discovery": True, **advanced}),
        bindings=[CaptureBinding[ResourceConfig](
            resourceConfig=ResourceConfig(database="shop", container="orders"), resourcePath=["shop", "orders"], stateKey="orders",
            collection=CollectionSpec(
                name="test/orders", key=["/customer", "/_meta/partitionKeyState", "/id"], writeSchema=ORDERS_SCHEMA,
            ),
        )],
    )


def open_replies(container=ORDERS):
    # Open lists containers but never samples partition keys.
    return [(200, {}, {}), (200, {"DocumentCollections": [container]}, {})]


def restore_state(records, previous=None):
    state = previous.model_dump(mode="json", exclude_unset=True) if previous else {}
    for record in records:
        if "checkpoint" in record:
            json_merge_patch(state, record["checkpoint"]["state"]["updated"])
    return ConnectorState.model_validate_json(json.dumps(state))


@pytest.mark.asyncio
@pytest.mark.parametrize("priority", [None, "Low", "High"])
@pytest.mark.parametrize("start_time", ["Beginning", "2026-11-11T04:05:06+02:00"])
async def test_ru_options_and_start_time_reach_sdk(monkeypatch, priority, start_time):
    advanced = {"start_time": start_time}
    if priority:
        advanced.update(priority=priority, throughput_bucket=3, max_item_count=17)
    transport = install_cosmos(monkeypatch, [
        *open_replies(), *sample_replies(ORDERS, ["acme"]),
        *open_replies(), *initialization(),
        *([caught_up('"0"')] if start_time != "Beginning" else []),
        page(ITEMS[:1], '"1"'), caught_up('"1"'),
        *([caught_up('"1"')] if start_time != "Beginning" else []),
    ], handle_health_checks=True)
    spec = capture_spec(advanced)
    # Discovery samples partition keys with the configured RU options.
    await Connector().discover(LOG, request.Discover[EndpointConfig](connectorType="IMAGE", config=spec.config))
    records = await capture_once(monkeypatch, spec, ConnectorState())
    assert [record["captured"]["doc"]["id"] for record in records if "captured" in record] == ["o1"]
    state = restore_state(records)
    assert state.bindingStateV1["orders"].inc.cursor[0]
    headers = feed_headers(transport)
    for header in headers:
        assert header.get("x-ms-cosmos-priority-level") == priority
        assert header.get("x-ms-max-item-count") == ("17" if priority else None)
    sampling = [
        headers for (method, _, _), headers in zip(transport.requests, transport.request_headers)
        if method == "POST"
    ]
    assert len(sampling) == 1
    assert sampling[0].get("x-ms-cosmos-priority-level") == priority
    assert sampling[0]["x-ms-max-item-count"] == "1"
    for header in transport.request_headers:
        assert header.get("x-ms-cosmos-throughput-bucket") == ("3" if priority else None)
    assert headers[0].get("if-none-match") is None
    assert headers[0].get("if-modified-since") == (
        None if start_time == "Beginning" else "Wed, 11 Nov 2026 02:05:06 GMT"
    )
    assert not transport.replies
    assert transport.closed

    transport = install_cosmos(monkeypatch, [
        *open_replies(), *initialization(),
        page([{**ITEMS[1], "status": "updated"}], '"2"'), caught_up('"2"'),
        *([caught_up('"2"')] if start_time != "Beginning" else []),
    ], handle_health_checks=True)
    # A saved continuation wins even if the configured start position changes.
    advanced["start_time"] = "Now" if start_time == "Beginning" else "Beginning"
    records = await capture_once(monkeypatch, capture_spec(advanced), state)
    captured = [record["captured"]["doc"] for record in records if "captured" in record]
    assert captured == [{**ITEMS[1], "status": "updated", "_meta": VALUE}]
    assert feed_headers(transport)[0]["if-none-match"] == '"1"'
    assert feed_headers(transport)[0].get("if-modified-since") == (
        None if start_time == "Beginning" else "Wed, 11 Nov 2026 02:05:06 GMT"
    )
    assert not transport.replies
    assert transport.closed


@pytest.mark.asyncio
async def test_now_empty_feed_checkpoints_start_and_survives_restarts(monkeypatch):
    class FirstStart(datetime):
        @classmethod
        def now(cls, tz):
            return cls(2026, 11, 11, 10, 11, 12, 123456, tzinfo=tz)

    monkeypatch.setattr(api, "datetime", FirstStart)
    transport = install_cosmos(monkeypatch, [
        *open_replies(), *initialization(), caught_up('"0"'), caught_up('"0"'),
    ], handle_health_checks=True)
    records = await capture_once(monkeypatch, capture_spec({"start_time": "Now"}), ConnectorState())
    assert not [record for record in records if "captured" in record]
    assert restore_state(records[:2]).bindingStateV1["orders"].inc.cursor == ("start_time:2026-11-11T10:11:12+00:00",)
    state = restore_state(records)
    cursor = state.bindingStateV1["orders"].inc.cursor
    assert cursor[0].startswith(api.SDK_CONTINUATION_PREFIX)
    token = ChangeFeedContinuation.model_validate_json(base64.b64decode(cursor[0][len(api.SDK_CONTINUATION_PREFIX):]))
    assert token.startFrom.PointInTimeMs == int(FirstStart.now(UTC).timestamp()) * 1000
    assert token.continuation.continuation[0].token == '"0"'
    assert feed_headers(transport)[0]["if-modified-since"] == "Wed, 11 Nov 2026 10:11:12 GMT"
    assert not transport.replies
    assert transport.closed

    class LaterStart(datetime):
        @classmethod
        def now(cls, tz):
            raise AssertionError("Now must not be recomputed from a saved checkpoint")

    monkeypatch.setattr(api, "datetime", LaterStart)
    for start_time in ("Now", "Beginning"):
        transport = install_cosmos(monkeypatch, [
            *open_replies(), *initialization(), caught_up('"0"'), caught_up('"0"'),
        ], handle_health_checks=True)
        records = await capture_once(monkeypatch, capture_spec({"start_time": start_time}), state.model_copy(deep=True))
        assert records == []
        assert feed_headers(transport)[0]["if-modified-since"] == "Wed, 11 Nov 2026 10:11:12 GMT"
        assert feed_headers(transport)[0]["if-none-match"] == '"0"'
        assert not transport.replies
        assert transport.closed

    changed = {"id": "after-start", "customer": "acme", "_ts": 1794391873}
    transport = install_cosmos(monkeypatch, [
        *open_replies(), *initialization(),
        page([changed], '"1"'), caught_up('"1"'), caught_up('"1"'),
    ], handle_health_checks=True)
    records = await capture_once(monkeypatch, capture_spec({"start_time": "Now"}), state)
    assert [record["captured"]["doc"] for record in records if "captured" in record] == [
        {**changed, "_meta": VALUE},
    ]
    state = restore_state(records, state)
    assert state.bindingStateV1["orders"].inc.cursor != cursor
    assert not transport.replies
    assert transport.closed

    transport = install_cosmos(monkeypatch, [
        *open_replies(), *initialization(), caught_up('"1"'), caught_up('"1"'),
    ], handle_health_checks=True)
    assert await capture_once(monkeypatch, capture_spec({"start_time": "Now"}), state) == []
    assert feed_headers(transport)[0]["if-none-match"] == '"1"'
    assert not transport.replies
    assert transport.closed


@pytest.mark.asyncio
async def test_now_checkpoint_precedes_first_request(monkeypatch):
    from logging import getLogger
    from unittest.mock import MagicMock

    source = MagicMock()
    fetch = api.fetch_container(
        source, "shop", "orders", getLogger(__name__), ("",), properties_cache={}, schema=ORDERS_SCHEMA, start_time="Now",
    )
    cursor = await anext(fetch)
    await fetch.aclose()
    assert cursor[0].startswith(api.START_TIME_CURSOR_PREFIX)
    assert datetime.fromisoformat(cursor[0][len(api.START_TIME_CURSOR_PREFIX):]).tzinfo == UTC
    source.get_database_client.assert_not_called()


@pytest.mark.asyncio
async def test_empty_timestamp_feed_keeps_saved_start_when_config_changes(monkeypatch):
    transport = install_cosmos(monkeypatch, [
        *open_replies(), *initialization(), caught_up('"0"'), caught_up('"0"'),
    ], handle_health_checks=True)
    records = await capture_once(monkeypatch, capture_spec({"start_time": "2026-11-11T02:05:06Z"}), ConnectorState())
    assert restore_state(records[:2]).bindingStateV1["orders"].inc.cursor == ("start_time:2026-11-11T02:05:06+00:00",)
    state = restore_state(records)
    assert not [record for record in records if "captured" in record]
    cursor = state.bindingStateV1["orders"].inc.cursor[0]
    assert cursor.startswith(api.SDK_CONTINUATION_PREFIX)
    token = ChangeFeedContinuation.model_validate_json(base64.b64decode(cursor[len(api.SDK_CONTINUATION_PREFIX):]))
    assert token.startFrom.PointInTimeMs == int(datetime(2026, 11, 11, 2, 5, 6, tzinfo=UTC).timestamp()) * 1000
    assert token.continuation.continuation[0].token == '"0"'
    assert not transport.replies
    assert transport.closed
    transport = install_cosmos(monkeypatch, [
        *open_replies(), *initialization(), caught_up('"0"'), caught_up('"0"'),
    ], handle_health_checks=True)
    assert await capture_once(monkeypatch, capture_spec({"start_time": "Now"}), state) == []
    assert feed_headers(transport)[0]["if-modified-since"] == "Wed, 11 Nov 2026 02:05:06 GMT"
    assert feed_headers(transport)[0]["if-none-match"] == '"0"'
    assert not transport.replies
    assert transport.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("start", ["Beginning", "2026-11-11T02:05:06Z"])
async def test_checkpoint_keeps_unread_ranges_and_repeated_resumes(monkeypatch, start):
    monkeypatch.setattr(api, "MAX_PAGES_PER_POLL", 1)
    ranges = TWO_RANGES
    state = ConnectorState()
    expected_ms = int(datetime(2026, 11, 11, 2, 5, 6, tzinfo=UTC).timestamp() * 1000)
    for index, range_id in enumerate(("0", "1", "0")):
        transport = install_cosmos(monkeypatch, [
            *open_replies(), *initialization(ranges=ranges),
            *([caught_up('"0"')] if index < 2 and start != "Beginning" else []),
            page([{"id": f"new-{index}", "customer": "acme"}], f'"{index + 1}"'),
        ], handle_health_checks=True)
        records = await capture_once(
            monkeypatch, capture_spec({"start_time": start if index == 0 else "Beginning"}),
            state.model_copy(deep=True),
        )
        assert [record["captured"]["doc"] for record in records if "captured" in record] == [
            {"id": f"new-{index}", "customer": "acme", "_meta": VALUE},
        ]
        state = restore_state(records, state)
        cursor = state.bindingStateV1["orders"].inc.cursor[0]
        assert cursor.startswith(api.SDK_CONTINUATION_PREFIX)
        token = ChangeFeedContinuation.model_validate_json(base64.b64decode(cursor[len(api.SDK_CONTINUATION_PREFIX):]))
        if start != "Beginning":
            assert token.startFrom.PointInTimeMs == expected_ms
        else:
            assert token.startFrom.Type == "Beginning"
        if index == 0:
            assert token.continuation.continuation[0].token is None
        for headers in feed_headers(transport):
            assert headers["x-ms-documentdb-partitionkeyrangeid"] == range_id
            assert headers.get("if-modified-since") == (None if start == "Beginning" else "Wed, 11 Nov 2026 02:05:06 GMT")
        assert feed_headers(transport)[0].get("if-none-match") == ('"1"' if index == 2 else None)
        assert not transport.replies
        assert transport.closed


@pytest.mark.asyncio
async def test_now_recreated_container_captures_items_before_detection(monkeypatch):
    class FirstStart(datetime):
        @classmethod
        def now(cls, tz):
            return cls(2026, 11, 11, 10, tzinfo=tz)

    monkeypatch.setattr(api, "datetime", FirstStart)
    transport = install_cosmos(monkeypatch, [
        *open_replies(), *initialization(), caught_up('"0"'),
        page([{"id": "old", "customer": "acme"}], '"1"'), caught_up('"1"'), caught_up('"1"'),
    ], handle_health_checks=True)
    spec = capture_spec({"start_time": "Now"})
    state = restore_state(await capture_once(monkeypatch, spec, ConnectorState()))
    assert not transport.replies
    assert transport.closed

    class DetectionTime(datetime):
        @classmethod
        def now(cls, tz):
            raise AssertionError("A recreated container must not resolve a new Now")

    monkeypatch.setattr(api, "datetime", DetectionTime)
    recreated = {**ORDERS, "_rid": "bmV3cmlkMDA="}
    transport = install_cosmos(monkeypatch, [
        *open_replies(recreated), *initialization(recreated),
    ], handle_health_checks=True)
    records = await capture_once(monkeypatch, spec, state.model_copy(deep=True))
    state = restore_state(records, state)
    assert not state.backfillRequests
    assert not feed_headers(transport)
    assert not transport.replies
    assert transport.closed

    items = [{"id": f"new-{index}", "customer": "acme", "_ts": 1794394800} for index in range(10)]
    transport = install_cosmos(monkeypatch, [
        *open_replies(recreated), *initialization(recreated), page(items, '"10"'), caught_up('"10"'),
    ], handle_health_checks=True)
    records = await capture_once(monkeypatch, spec, state.model_copy(deep=True))
    assert [record["captured"]["doc"] for record in records if "captured" in record] == [
        {**item, "_meta": VALUE} for item in items
    ]
    assert feed_headers(transport)[0].get("if-modified-since") is None
    assert feed_headers(transport)[0].get("if-none-match") is None
    assert not transport.replies
    assert transport.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("start", ["Now", "2026-11-11T02:05:06Z"])
async def test_empty_timestamp_initialization_preserves_second_range(monkeypatch, start):
    ranges = TWO_RANGES
    item = {"id": "second-range", "customer": "globex"}
    delivered = False

    class RangeTransport(FakeTransport):
        async def send(self, request, **kwargs):
            nonlocal delivered
            if request.method == "GET" and request.url.endswith("/docs/"):
                range_id = request.headers["x-ms-documentdb-partitionkeyrangeid"]
                etag = request.headers.get("If-None-Match")
                if range_id == "0":
                    self.replies.append(caught_up('"a"'))
                elif etag is None:
                    self.replies.append(caught_up('"b"'))
                elif etag == '"b"':
                    self.replies.append(page([item], '"c"'))
                    delivered = True
                else:
                    assert etag == '"c"'
                    self.replies.append(caught_up('"c"'))
            return await super().send(request, **kwargs)

    state = ConnectorState()
    for _ in range(2):
        transport = RangeTransport([*open_replies(), *initialization(ranges=ranges)], handle_health_checks=True)
        install_cosmos(monkeypatch, transport)
        records = await capture_once(monkeypatch, capture_spec({"start_time": start}), state.model_copy(deep=True))
        state = restore_state(records, state)
        assert not transport.replies
        assert transport.closed
        if delivered:
            break
    assert delivered, "The second range must be read by the second poll"
    assert [record["captured"]["doc"] for record in records if "captured" in record] == [
        {**item, "_meta": VALUE},
    ]
    assert any(
        header["x-ms-documentdb-partitionkeyrangeid"] == "1" and header.get("if-none-match") == '"b"'
        for header in feed_headers(transport)
    )
