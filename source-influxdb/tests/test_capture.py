import asyncio
import csv
import io
import json
import logging
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, Mock
from weakref import ref

import pytest
from estuary_cdk.capture import Task, _emit, common, request
from estuary_cdk.capture.common import ResourceState
from estuary_cdk.flow import CaptureBinding
from estuary_cdk.capture.transactor import Transactor
from estuary_cdk.http import HTTPError
from estuary_cdk.incremental_csv_processor import CSVProcessingError
from estuary_cdk.utils import json_merge_patch
from freezegun import freeze_time
from pydantic import ValidationError

from source_influxdb import Connector
from source_influxdb import api, resources
from source_influxdb.api import (
    WINDOW_SIZE,
    datetime_to_ns,
    fetch_points,
    format_timestamp,
    parse_field_value,
    parse_timestamp,
    read_points,
    series_key,
)
from source_influxdb.models import ConnectorState, EndpointConfig, Point, ResourceConfig

LOG = logging.getLogger("test")
START = "2026-11-10T12:00:00Z"
STOP = "2026-11-10T14:59:00Z"
FIXTURES = Path(__file__).parent / "fixtures"


def stream(data, chunk_size=7):
    async def body():
        for offset in range(0, len(data), chunk_size):
            yield data[offset:offset + chunk_size]

    return {}, body


async def collect_async(gen):
    return [item async for item in gen]


def collect(gen):
    return asyncio.run(collect_async(gen))


def table(rows, datatype="double", tags=("host", "region"), defaults=None):
    output = io.StringIO()
    writer = csv.writer(output)
    columns = ["", "result", "table", "_time", "_value", "_field", "_measurement", *tags]
    writer.writerow(["#datatype", "string", "long", "dateTime:RFC3339", datatype, "string", "string", *["string"] * len(tags)])
    writer.writerow(["#group", "false", "false", "false", "false", "true", "true", *["true"] * len(tags)])
    writer.writerow(["#default", "_result", *[defaults.get(c, "") if defaults else "" for c in columns[2:]]])
    writer.writerow(columns)
    writer.writerows(rows)
    return output.getvalue().encode()


def points(http, config, measurement="readings", start=START, stop=STOP):
    return asyncio.run(read_points(
        LOG, http, config.url, config.org, config.common_headers,
        "sensors", measurement, parse_timestamp(start), parse_timestamp(stop),
    ))


def capture_once(http, config, monkeypatch, state=None, names=("readings", "status"), *, finish_sweep=False, resource_configs=None):
    connector = Connector()
    monkeypatch.setattr(connector, "request", http.request)
    monkeypatch.setattr(connector, "request_stream", http.request_stream)
    opened = request.Open[EndpointConfig, ResourceConfig, ConnectorState].model_validate({
        "capture": {
            "name": "test/capture", "connectorType": "IMAGE", "config": config, "intervalSeconds": 300,
            "bindings": [{
                "resourceConfig": resource_configs[name] if resource_configs else {"name": f"sensors.{name}", "interval": "PT5M"},
                "resourcePath": [resource_configs[name]["name"] if resource_configs else f"sensors.{name}"], "stateKey": name,
                "collection": {"name": f"test/{name}", "key": ["/series", "/time"], "writeSchema": {}},
            } for name in names],
        },
        "state": state or {}, "version": "test", "range": {"keyBegin": 0, "keyEnd": 4294967295, "rClockBegin": 0, "rClockEnd": 4294967295},
    })
    output = io.BytesIO()

    async def run():
        stopping = Task.Stopping()
        finished = set()

        # Stop after each binding's first window, including an empty poll. The
        # CDK consumes the final cursor and commits it before this barrier.
        async def one_window(*args, **kwargs):
            cursor = args[-1]
            while True:
                async for item in fetch_points(*args[:-1], cursor, **kwargs):
                    if isinstance(item, tuple):
                        cursor = item
                    yield item
                if not finish_sweep or kwargs["fetch_state"].last_caught_up_at is not None:
                    break
            finished.add(args[5])
            if finished == set(names):
                stopping.event.set()
            await stopping.event.wait()

        with monkeypatch.context() as patch:
            # Each resumed capture runs on a new event loop, like a new process.
            patch.setattr(_emit, "_emit_lock", asyncio.Lock())
            patch.setattr(api, "WINDOW_READ_SEMAPHORE", asyncio.Semaphore(api.MAX_CONCURRENT_READS))
            patch.setattr(resources, "fetch_points", one_window)
            response, capture = await connector.open(LOG, opened)
            assert response.explicitAcknowledgements is False
            async with asyncio.timeout(5), asyncio.TaskGroup() as tg:
                task = Task(
                    log=LOG, connector_status=Mock(), name="test", output=output,
                    stopping=stopping, tg=tg, transactor=Transactor(output),
                    catalog_task_name="test/capture",
                )
                await capture(task)
            assert stopping.first_error is None
            assert finished == set(names)

    asyncio.run(run())
    return [json.loads(line) for line in output.getvalue().splitlines()]


def committed_state(records, state=None):
    state = json.loads(json.dumps(state or {}))
    for record in records:
        if "checkpoint" in record:
            checkpoint = record["checkpoint"]["state"]
            assert checkpoint["mergePatch"] is True
            json_merge_patch(state, checkpoint["updated"])
    return state


@pytest.fixture
def point_server(http, config):
    stored = []
    discovery_response = http.request_stream.return_value

    async def query(log, url, **kwargs):
        query = kwargs["json"]["query"]
        if 'distinct(column: "_measurement")' in query:
            return discovery_response
        start, stop = map(int, re.search(r"range\(start: time\(v: (-?\d+)\), stop: time\(v: (-?\d+)\)\)", query).groups())
        assert start < stop <= datetime_to_ns(datetime.now(tz=UTC) - timedelta(seconds=config.advanced.lag_seconds))
        measurement = json.loads(re.search(r'r\["_measurement"\] == (.+)\)', query).group(1))
        selected = [
            (time, tags, fields) for name, time, tags, fields in stored
            if name == measurement and start <= parse_timestamp(time) < stop
        ]
        if "|> first()" in query:
            first = min((parse_timestamp(time) for time, _, _ in selected), default=None)
            return stream(b"" if first is None else f",result,table,_time\n,,0,{format_timestamp(first)}\n".encode())

        assert stop - start <= int(WINDOW_SIZE.total_seconds()) * 1_000_000_000
        data = b""
        # InfluxDB tables are grouped by series and field, not globally by time.
        for time, tags, fields in reversed(selected):
            for field, value in fields.items():
                datatype = {float: "double", int: "long", bool: "boolean", str: "string"}[type(value)]
                value = str(value).lower() if isinstance(value, bool) else str(value)
                data += table([
                    ["", "", "0", time, value, field, measurement, *tags.values()],
                ], datatype=datatype, tags=tuple(tags))
        return stream(data)

    http.request_stream.side_effect = query
    return stored


@pytest.mark.parametrize("measurement, count", [("readings", 3), ("status", 1)])
def test_seed_points(http, config, measurement, count):
    http.request_stream.return_value = stream((FIXTURES / f"{measurement}.csv").read_bytes())
    docs = points(http, config, measurement)

    assert len(docs) == count
    if measurement == "readings":
        assert [doc.fields for doc in docs] == [
            {"value": 1.0, "ok": True},
            {"value": 2.0, "ok": True},
            {"value": 3.0, "ok": False},
        ]
        assert all(type(doc.fields["value"]) is float for doc in docs)
        assert all(type(doc.fields["ok"]) is bool for doc in docs)
        assert all(doc.tags == {"host": "a", "region": "west"} for doc in docs)
        assert all(doc.series == "readings,host=a,region=west" for doc in docs)
    else:
        assert docs[0].fields == {"count": 7, "state": "up"}
        assert type(docs[0].fields["count"]) is int
        assert docs[0].tags == {"host": "a"}
        assert docs[0].series == "status,host=a"

    call = http.request_stream.await_args
    assert call.args == (LOG, config.url + "/api/v2/query")
    assert call.kwargs["method"] == "POST"
    assert call.kwargs["params"] == {"org": config.org}
    assert call.kwargs["headers"] == {**config.common_headers, "Accept": "application/csv"}
    assert call.kwargs["json"]["dialect"] == {
        "header": True, "annotations": ["datatype", "group", "default"], "dateTimeFormat": "RFC3339Nano",
    }


def test_pivot_series_and_nanoseconds(http, config):
    t1 = "2026-11-10T12:00:00.000000001Z"
    t2 = "2026-11-10T12:00:00.000000002Z"
    data = table([
        ["", "", "0", t2, "2", "value", "readings", "b", "east"],
        ["", "", "0", t1, "1", "value", "readings", "b", "east"],
        ["", "", "1", t1, "3", "value", "readings", "a", "west"],
    ]) + table([
        ["", "", "2", t1, "false", "ok", "readings", "a", "west"],
        ["", "", "3", t2, "true", "ok", "readings", "b", "east"],
    ], datatype="boolean")
    http.request_stream.return_value = stream(data, chunk_size=1)

    docs = points(http, config)

    assert [(doc.time, doc.series, doc.fields) for doc in docs] == [
        (t1, "readings,host=a,region=west", {"value": 3.0, "ok": False}),
        (t1, "readings,host=b,region=east", {"value": 1.0}),
        (t2, "readings,host=b,region=east", {"value": 2.0, "ok": True}),
    ]


def test_changing_schemas_defaults_and_quoted_strings(http, config):
    value = 'a comma, a "quote", a newline\n°'
    data = table([
        ["", "", "0", START, value, "note", "readings", "", "west"],
        ["", "", "0", "2026-11-10T12:01:00Z", "", "note", "readings", "", "west"],
    ], datatype="string", defaults={"host": "a"})
    data += table([
        ["", "", "1", START, "7", "count", "readings"],
    ], datatype="long", tags=())
    data += table([
        ["", "", "2", START, "true", "ok", "readings", ""],
    ], datatype="boolean", tags=("error",))
    http.request_stream.return_value = stream(data, chunk_size=1)

    docs = points(http, config)

    assert [(doc.tags, doc.fields) for doc in docs] == [
        ({}, {"count": 7, "ok": True}),
        ({"host": "a", "region": "west"}, {"note": value}),
        ({"host": "a", "region": "west"}, {"note": ""}),
    ]


@pytest.mark.parametrize("tag", ["table", "result"])
def test_protocol_column_names_are_preserved_as_tags(http, config, tag):
    http.request_stream.return_value = stream(table([
        ["", "", "0", START, "1", "value", "readings", "a", "first"],
        ["", "", "1", START, "2", "value", "readings", "a", "second"],
    ], tags=("host", tag)))

    docs = points(http, config)

    assert [doc.tags for doc in docs] == [{"host": "a", tag: value} for value in ("first", "second")]
    assert [doc.series for doc in docs] == [f"readings,host=a,{tag}={value}" for value in ("first", "second")]
    assert [doc.fields for doc in docs] == [{"value": 1.0}, {"value": 2.0}]


def test_duplicate_data_columns_are_rejected(http, config):
    http.request_stream.return_value = stream(table([], tags=("host", "host")))
    with pytest.raises(CSVProcessingError, match="Duplicate columns"):
        points(http, config)


def test_query_stream_has_no_total_timeout(http, config):
    http.request_stream.return_value = stream(table([]))
    assert points(http, config) == []
    timeout = http.request_stream.await_args.kwargs["timeout"]
    assert timeout.total is None
    assert 0 < timeout.sock_read < float("inf")


@pytest.mark.parametrize("datatype, value, expected", [
    ("double", "1", 1.0),
    ("double", "-1.25e2", -125.0),
    ("long", "-9223372036854775808", -(2**63)),
    ("long", "9223372036854775807", 2**63 - 1),
    ("unsignedLong", "18446744073709551615", 2**64 - 1),
    ("boolean", "true", True),
    ("boolean", "false", False),
    ("string", "", ""),
    ("string", "false", "false"),
    ("string", "7", "7"),
])
def test_field_types(datatype, value, expected):
    actual = parse_field_value(value, datatype)
    assert actual == expected
    assert type(actual) is type(expected)


@pytest.mark.parametrize("datatype, value", [
    ("boolean", "maybe"), ("boolean", "1"), ("long", "1.5"),
    ("long", "9223372036854775808"), ("unsignedLong", "-1"),
    ("unsignedLong", "18446744073709551616"),
    ("double", "NaN"), ("double", "+Inf"), ("double", "-Inf"),
    ("double", ""), ("long", ""), ("unknown", "1"),
])
def test_invalid_field_types(datatype, value):
    with pytest.raises(ValueError):
        parse_field_value(value, datatype)


@pytest.mark.parametrize("value, expected", [
    (START, START),
    ("2026-11-10T12:00:00.000000000Z", START),
    ("2026-11-10T12:00:00.123456789Z", "2026-11-10T12:00:00.123456789Z"),
    ("2026-11-10T14:00:00.12+02:00", "2026-11-10T12:00:00.120000000Z"),
    ("2026-11-10T04:00:00.000000001-08:00", "2026-11-10T12:00:00.000000001Z"),
    ("1969-12-31T23:59:59.999999999Z", "1969-12-31T23:59:59.999999999Z"),
])
def test_timestamp_precision(value, expected):
    assert format_timestamp(parse_timestamp(value)) == expected


@pytest.mark.parametrize("value", ["bad", "2026-11-10T12:00:00", "2026-11-10T12:00:00.1234567890Z"])
def test_invalid_timestamps(value):
    with pytest.raises(ValueError):
        parse_timestamp(value)


def test_series_key():
    assert series_key("readings", {}) == "readings"
    assert series_key("readings", {"region": "west", "host": "a"}) == "readings,host=a,region=west"
    assert series_key("readings", {"host": "a,region=west"}) != series_key("readings", {"host": "a", "region": "west"})


@pytest.mark.parametrize("data", [b"", b"\r\n", table([])])
def test_empty_window(http, config, data):
    http.request_stream.return_value = stream(data)
    assert points(http, config) == []


@pytest.mark.parametrize("suffix", [
    b",error,reference\r\n,query failed,123\r\n",
    b"#datatype,string,long\n",
    b",result,table,_time,_value,_field,_measurement,host,region\n,,0,missing\n",
])
def test_partial_response_never_emits_points(http, config, suffix):
    good = table([["", "", "0", START, "1", "value", "readings", "a", "west"]])
    http.request_stream.return_value = stream(good + suffix)
    with pytest.raises((CSVProcessingError, RuntimeError)):
        points(http, config)


@pytest.mark.parametrize("data, error", [
    (b",error,reference\n,query failed,123\n", RuntimeError),
    (b",result,table,_time,_value,_field,_measurement\n", CSVProcessingError),
    (table([["", "", "0", START, "1", "value", "readings", "a"]]), CSVProcessingError),
    (table([["", "", "0", START, "1", "value", "readings", "a", "west", "extra"]]), CSVProcessingError),
    (table([["", "", "0", START, "1", "value", "readings", "a", "west"]]).replace(b"_field", b"missing"), ValidationError),
    (table([["", "", "0", START, "maybe", "value", "readings", "a", "west"]], datatype="boolean"), ValueError),
])
def test_invalid_points_fail(http, config, data, error):
    http.request_stream.return_value = stream(data)
    with pytest.raises(error):
        points(http, config)


def test_capture_http_errors_propagate(http, config):
    error = HTTPError("InfluxDB request failed", 403)
    http.request_stream.side_effect = error
    with pytest.raises(HTTPError) as raised:
        points(http, config)
    assert raised.value is error


@freeze_time("2026-11-10T15:00:00Z")
@pytest.mark.parametrize("page", [None, 1632268800000000000])
def test_incremental_seeks_across_empty_history(http, config, page):
    first = parse_timestamp(START) + 1
    first_time = format_timestamp(first)
    boundary = first + int(WINDOW_SIZE.total_seconds()) * 1_000_000_000
    http.request_stream.side_effect = [
        stream(f",result,table,_time\n,,0,{first_time}\n".encode()),
        stream(table([
            ["", "", "0", first_time, "1", "value", "readings", "a", "west"],
            ["", "", "1", first_time, "2", "value", "readings", "b", "east"],
        ]) + table([
            ["", "", "2", first_time, "true", "ok", "readings", "a", "west"],
        ], datatype="boolean")),
    ]
    start = parse_timestamp(config.start_date) if page is None else page

    emitted = collect(fetch_points(
        http, config.url, config.org, config.common_headers, 'sensors."west"', "readings",
        config.advanced.lag_seconds, LOG, (format_timestamp(start - 1),),
    ))

    assert len(emitted) == 3
    assert [(doc.time, doc.tags, doc.fields) for doc in emitted[:-1]] == [
        (first_time, {"host": "a", "region": "west"}, {"value": 1.0, "ok": True}),
        (first_time, {"host": "b", "region": "east"}, {"value": 2.0}),
    ]
    assert emitted[-1] == (first_time,)

    lookup, read = http.request_stream.await_args_list
    assert lookup.args == (LOG, config.url + "/api/v2/query")
    assert lookup.kwargs["method"] == "POST"
    assert lookup.kwargs["params"] == {"org": config.org}
    assert lookup.kwargs["headers"] == {**config.common_headers, "Accept": "application/csv"}
    assert lookup.kwargs["json"]["dialect"] == {
        "header": True, "annotations": [], "dateTimeFormat": "RFC3339Nano",
    }
    assert lookup.kwargs["json"]["query"] == (
        'from(bucket: "sensors.\\"west\\"")\n'
        f"  |> range(start: time(v: {start}), stop: time(v: {parse_timestamp(STOP)}))\n"
        '  |> filter(fn: (r) => r["_measurement"] == "readings")\n'
        '  |> first()\n'
        '  |> keep(columns: ["_time"])\n'
        '  |> group()\n'
        '  |> sort(columns: ["_time"])\n'
        '  |> limit(n: 1)'
    )
    assert f"range(start: time(v: {first}), stop: time(v: {boundary}))" in read.kwargs["json"]["query"]


@freeze_time("2026-11-10T15:00:00Z")
@pytest.mark.parametrize("data", [b"", b"\r\n", b",result,table,_time\n"])
def test_empty_long_history_finishes(http, config, data):
    http.request_stream.return_value = stream(data)

    assert changes(http, config, (format_timestamp(parse_timestamp(config.start_date) - 1),)) == []
    http.request_stream.assert_awaited_once()


@freeze_time("2026-11-10T15:00:00Z")
@pytest.mark.parametrize("data, error", [
    (b",error,reference\n,query failed,123\n", ValidationError),
    (b",result,table,_time\n,,0,invalid\n", ValueError),
    (b",result,table,_time\n,,0,2019-12-31T23:59:59.999999999Z\n", ValueError),
    (f",result,table,_time\n,,0,{STOP}\n".encode(), ValueError),
    (f",result,table,_time\n,,0,{START}\n,,0,{START}\n".encode(), ValueError),
    (f",result,table,_time\n,,0,{START}\n,error,reference\n".encode(), CSVProcessingError),
])
def test_invalid_first_point_never_checkpoints(http, config, data, error):
    http.request_stream.return_value = stream(data)
    emitted = []

    async def run():
        async for item in fetch_points(
            http, config.url, config.org, config.common_headers, "sensors", "readings",
            config.advanced.lag_seconds, LOG, (format_timestamp(parse_timestamp(config.start_date) - 1),),
        ):
            emitted.append(item)

    with pytest.raises(error):
        asyncio.run(run())
    assert emitted == []
    http.request_stream.assert_awaited_once()


@freeze_time("2026-11-10T15:00:00Z")
def test_dense_catch_up_skips_repeat_lookups(http, config, point_server):
    hour = int(WINDOW_SIZE.total_seconds()) * 1_000_000_000
    start = parse_timestamp(STOP) - 72 * hour
    point_server.extend(
        ("readings", format_timestamp(start + i * hour), {}, {"value": float(i)}) for i in range(3)
    )
    state = api.FetchState()
    cursor = (format_timestamp(start - 1),)
    queries = []
    while emitted := changes(http, config, cursor, fetch_state=state):
        cursor = emitted[-1]
        queries.append(["first" if "|> first()" in call.kwargs["json"]["query"] else "window"
                        for call in http.request_stream.await_args_list])
        http.request_stream.reset_mock(return_value=False, side_effect=False)

    # One lookup to find the history, then one query per window with points.
    assert queries == [["first", "window"], ["window"], ["window"]]
    assert cursor == (format_timestamp(start + 2 * hour),)
    # Once a window comes back empty, the next lookup skips the gap.
    calls = [call.kwargs["json"]["query"] for call in http.request_stream.await_args_list]
    assert ["|> first()" in query for query in calls] == [False, True]


@freeze_time("2026-11-10T15:00:00Z", real_asyncio=True)
def test_first_point_lookups_share_read_limit(http, config, monkeypatch):
    active = 0
    peak = 0

    async def query(log, url, **kwargs):
        assert "|> first()" in kwargs["json"]["query"]

        async def body():
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            try:
                await asyncio.sleep(0.01)
                yield b""
            finally:
                active -= 1

        return {}, body

    http.request_stream.side_effect = query

    async def run():
        monkeypatch.setattr(api, "WINDOW_READ_SEMAPHORE", asyncio.Semaphore(api.MAX_CONCURRENT_READS))
        await asyncio.gather(*(
            collect_async(fetch_points(
                http, config.url, config.org, config.common_headers, "sensors", f"metric_{i}",
                config.advanced.lag_seconds, LOG, (format_timestamp(parse_timestamp(config.start_date) - 1),),
            )) for i in range(12)
        ))

    asyncio.run(run())
    assert http.request_stream.await_count == 12
    assert 1 < peak <= api.MAX_CONCURRENT_READS


@freeze_time("2026-11-10T15:00:00Z", real_asyncio=True)
def test_capture_seed_a(http, config, monkeypatch, snapshot):
    discovery_response = http.request_stream.return_value

    async def query(log, url, **kwargs):
        query = kwargs["json"]["query"]
        if 'distinct(column: "_measurement")' in query:
            return discovery_response
        start, stop = map(int, re.search(r"range\(start: time\(v: (-?\d+)\), stop: time\(v: (-?\d+)\)\)", query).groups())
        assert stop <= parse_timestamp(STOP)
        if "|> first()" in query:
            assert start <= parse_timestamp(START) < stop
            assert '|> keep(columns: ["_time"])' in query
            assert '|> group()' in query
            assert '|> sort(columns: ["_time"])' in query
            assert '|> limit(n: 1)' in query
            return stream(f",result,table,_time\n,,0,{START}\n".encode())
        assert stop - start <= int(WINDOW_SIZE.total_seconds()) * 1_000_000_000
        if stop <= parse_timestamp(START):
            return stream(b"")
        measurement = "readings" if '== "readings"' in query else "status"
        return stream((FIXTURES / f"{measurement}.csv").read_bytes())

    http.request_stream.side_effect = query
    records = capture_once(http, config, monkeypatch)
    docs = [record["captured"] for record in records if "captured" in record]
    docs.sort(key=lambda doc: (doc["binding"], doc["doc"]["time"]))
    assert len(docs) == 4
    assert snapshot("stdout.json") == docs
    finished = {
        key for record in records if "checkpoint" in record
        for key, state in record["checkpoint"]["state"]["updated"].get("bindingStateV1", {}).items()
        if "backfill" in state and state["backfill"] is None
    }
    assert finished == {"readings", "status"}
    # A timestamp lookup and a bounded read per measurement.
    # Walking every empty hour from 2020 exhausts the preview before any points.
    assert http.request_stream.await_count == 4
    state = committed_state(records)["bindingStateV1"]
    assert state["readings"]["inc"]["cursor"] == ["2026-11-10T12:02:00Z"]
    assert state["status"]["inc"]["cursor"] == [START]


@freeze_time("2026-11-10T15:00:00Z", real_asyncio=True)
def test_capture_resumes_committed_state(http, config, monkeypatch, point_server):
    point_server.extend([
        ("readings", START, {"host": "a", "region": "west"}, {"value": 1.0, "ok": True}),
        ("readings", "2026-11-10T12:01:00Z", {"host": "a", "region": "west"}, {"value": 2.0, "ok": True}),
        ("readings", "2026-11-10T12:02:00Z", {"host": "a", "region": "west"}, {"value": 3.0, "ok": False}),
        ("status", START, {"host": "a"}, {"state": "up", "count": 7}),
    ])
    first = capture_once(http, config, monkeypatch)
    assert len([r for r in first if "captured" in r]) == 4
    state = committed_state(first)
    assert state["bindingStateV1"]["readings"]["inc"]["cursor"] == ["2026-11-10T12:02:00Z"]
    assert state["bindingStateV1"]["status"]["inc"]["cursor"] == [START]

    # These timestamps precede run 1's cutoff, but follow its newest points.
    point_server.extend([
        ("readings", "2026-11-10T13:00:00Z", {"host": "b", "region": "east"}, {"value": 4.0, "ok": True}),
        ("readings", "2026-11-10T13:01:00Z", {"host": "b", "region": "east"}, {"value": 5.0, "ok": True}),
    ])
    second = capture_once(http, config, monkeypatch, state)
    docs = [r["captured"] for r in second if "captured" in r]
    assert len(docs) == 2
    assert all(doc["binding"] == 0 for doc in docs)
    assert [(doc["doc"]["time"], doc["doc"]["fields"]) for doc in docs] == [
        ("2026-11-10T13:00:00Z", {"value": 4.0, "ok": True}),
        ("2026-11-10T13:01:00Z", {"value": 5.0, "ok": True}),
    ]
    assert all(doc["doc"]["measurement"] == "readings" for doc in docs)
    assert all(doc["doc"]["tags"] == {"host": "b", "region": "east"} for doc in docs)
    assert all(doc["doc"]["series"] == "readings,host=b,region=east" for doc in docs)
    resumed_state = committed_state(second, state)
    assert resumed_state["bindingStateV1"]["readings"]["inc"]["cursor"] == ["2026-11-10T13:01:00Z"]
    assert resumed_state["bindingStateV1"]["status"] == state["bindingStateV1"]["status"]

    third = capture_once(http, config, monkeypatch, resumed_state)
    assert not any("captured" in r for r in third)
    assert committed_state(third, resumed_state) == resumed_state


def changes(http, config, cursor, **kwargs):
    return collect(fetch_points(
        http, config.url, config.org, config.common_headers, "sensors", "readings",
        config.advanced.lag_seconds, LOG, cursor,
        lookback_hours=config.advanced.lookback_hours, start_date=config.start_date, **kwargs,
    ))


@freeze_time("2026-11-10T15:00:00Z")
def test_default_history_uses_small_windows(http, config, point_server):
    config = EndpointConfig.model_validate(config.model_dump(exclude={"start_date"}))
    timestamp = format_timestamp(parse_timestamp(config.start_date))
    point_server.extend([
        ("readings", timestamp, {}, {"value": 1.0}),
        ("readings", START, {}, {"value": 2.0}),
    ])

    emitted = changes(http, config, (format_timestamp(parse_timestamp(timestamp) - 1),))

    assert len(emitted) == 2
    assert emitted[0].time == timestamp
    assert emitted[-1] == (timestamp,)
    query = http.request_stream.await_args.kwargs["json"]["query"]
    start, stop = map(int, re.search(r"range\(start: time\(v: (-?\d+)\), stop: time\(v: (-?\d+)\)\)", query).groups())
    assert stop - start <= 3600 * 1_000_000_000
    assert "|> sort(" not in query

    serve_query = http.request_stream.side_effect

    async def interrupted_query(log, url, **kwargs):
        headers, body = await serve_query(log, url, **kwargs)
        if "|> first()" in kwargs["json"]["query"]:
            return headers, body

        async def interrupted_body():
            async for chunk in body():
                yield chunk
            raise asyncio.TimeoutError

        return headers, interrupted_body

    http.request_stream.side_effect = interrupted_query
    with pytest.raises(asyncio.TimeoutError):
        changes(http, config, emitted[-1])
    http.request_stream.side_effect = serve_query
    resumed = changes(http, config, emitted[-1])
    assert len(resumed) == 2
    assert resumed[0].time == START
    assert resumed[-1] == (START,)


@freeze_time("2026-11-10T15:00:00Z")
def test_dense_windows_are_bounded_and_resume_complete_timestamps(http, config, monkeypatch):
    monkeypatch.setattr(api, "MAX_ROWS", 8, raising=False)
    start = parse_timestamp(STOP) - 12 * 1_000_000_000
    consumed = []
    closed = []

    async def query(log, url, **kwargs):
        query = kwargs["json"]["query"]
        lower, upper = map(int, re.search(r"range\(start: time\(v: (-?\d+)\), stop: time\(v: (-?\d+)\)\)", query).groups())
        read = len(consumed)
        consumed.append(0)

        async def body():
            try:
                # Field tables arrive in reverse time order, with fields far apart.
                for field in ("value", "other"):
                    for second in reversed(range(12)):
                        timestamp = start + second * 1_000_000_000
                        if lower <= timestamp < upper:
                            for host in ("a", "b"):
                                consumed[read] += 1
                                yield table([["", "", "0", format_timestamp(timestamp), "1", field, "readings", host]], tags=("host",))
            finally:
                closed.append(read)

        return {}, body

    http.request_stream.side_effect = query
    cursor = (format_timestamp(start - 1),)
    docs = []
    while emitted := changes(http, config, cursor):
        assert len(emitted[:-1]) <= 4
        assert all(doc.fields == {"value": 1.0, "other": 1.0} for doc in emitted[:-1])
        assert emitted[-2].time == emitted[-1][0]
        docs.extend(emitted[:-1])
        cursor = ResourceState.Incremental.model_validate_json(
            ResourceState.Incremental(cursor=emitted[-1]).model_dump_json()
        ).cursor

    assert [(doc.time, doc.tags["host"]) for doc in docs] == [
        (format_timestamp(start + second * 1_000_000_000), host)
        for second in range(12) for host in ("a", "b")
    ]
    assert max(consumed) <= 9
    assert sorted(closed) == list(range(len(consumed)))


@freeze_time("2026-11-10T15:00:00Z", real_asyncio=True)
def test_dense_bindings_limit_concurrent_reads(http, config, monkeypatch):
    config = EndpointConfig.model_validate({**config.model_dump(), "start_date": START})
    monkeypatch.setattr(api, "MAX_ROWS", 8)
    names = tuple(f"metric_{i}" for i in range(40))
    active = 0
    peak = 0
    consumed = []
    # Each read waits briefly for a second one to start, so the overlap does not
    # depend on scheduling; reads run one at a time leave peak at 1 and fail.
    overlapped = asyncio.Event()

    async def query(log, url, **kwargs):
        query = kwargs["json"]["query"]
        if "|> first()" in query:
            return stream(f",result,table,_time\n,,0,{START}\n".encode())
        measurement = re.search(r'== "([^"]+)"', query).group(1)
        start, stop = map(int, re.search(r"range\(start: time\(v: (-?\d+)\), stop: time\(v: (-?\d+)\)\)", query).groups())

        async def body():
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            rows = 0
            try:
                if active > 1:
                    overlapped.set()
                try:
                    await asyncio.wait_for(overlapped.wait(), 5)
                except asyncio.TimeoutError:
                    pass
                for second in range(0, 3600, 300):
                    timestamp = parse_timestamp(START) + second * 1_000_000_000
                    if start <= timestamp < stop:
                        await asyncio.sleep(0)
                        rows += 1
                        yield table([["", "", "0", format_timestamp(timestamp), "1", "value", measurement, "a", "west"]])
            finally:
                active -= 1
                consumed.append(rows)

        return {}, body

    http.request_stream.side_effect = query
    records = capture_once(http, config, monkeypatch, names=names)
    docs = [record["captured"] for record in records if "captured" in record]
    assert 1 < peak <= 4
    assert active == 0
    assert max(consumed) == 9
    assert len(docs) == 40 * 6
    assert {doc["doc"]["measurement"] for doc in docs} == set(names)
    state = committed_state(records)["bindingStateV1"]
    assert all(state[name]["inc"]["cursor"] == ["2026-11-10T12:25:00Z"] for name in names)


@freeze_time("2026-11-10T15:00:00Z")
def test_checkpoint_does_not_retain_window(http, config, monkeypatch):
    references = []

    async def read_window(*args):
        points = [
            Point(time=START, measurement="readings", tags={"host": str(i)}, fields={"value": 1.0}, series=f"readings,host={i}")
            for i in range(8)
        ]
        references.extend(ref(point) for point in points)
        return parse_timestamp(STOP), points

    monkeypatch.setattr(api, "read_window", read_window)

    async def run():
        count = 0
        async for item in fetch_points(
            http, config.url, config.org, config.common_headers, "sensors", "readings",
            config.advanced.lag_seconds, LOG, (format_timestamp(parse_timestamp(START) - 1),),
        ):
            if isinstance(item, Point):
                count += 1
            else:
                assert item == (START,)
                assert sum(point() is not None for point in references) <= 1
        assert count == 8

    asyncio.run(run())


@freeze_time("2026-11-10T15:00:00Z", real_asyncio=True)
@pytest.mark.parametrize("start_date", [
    "2026-11-10T12:00:00.123456789Z", "2026-11-10T14:00:00.123456789+02:00",
    "1969-12-31T23:59:59.123456789Z",
])
def test_start_date_nanoseconds_are_inclusive(http, config, monkeypatch, point_server, start_date):
    config = EndpointConfig.model_validate({**config.model_dump(), "start_date": start_date})
    start = parse_timestamp(start_date)
    point_server.extend([
        ("readings", format_timestamp(timestamp), {}, {"value": float(i)})
        for i, timestamp in enumerate((start - 289, start, start + 1))
    ])

    records = capture_once(http, config, monkeypatch, names=("readings",))
    docs = [record["captured"]["doc"] for record in records if "captured" in record]
    assert [(doc["time"], doc["fields"]) for doc in docs] == [
        (format_timestamp(start), {"value": 1.0}),
        (format_timestamp(start + 1), {"value": 2.0}),
    ]
    state = committed_state(records)
    assert state["bindingStateV1"]["readings"]["inc"]["cursor"] == [format_timestamp(start + 1)]
    resumed = capture_once(http, config, monkeypatch, state, names=("readings",))
    assert not any("captured" in record for record in resumed)


@freeze_time("2026-11-10T15:00:00Z")
def test_large_timestamp_is_not_split(http, config, monkeypatch, point_server):
    monkeypatch.setattr(api, "MAX_ROWS", 3)
    start = parse_timestamp(STOP) - 2
    point_server.extend([
        ("readings", format_timestamp(timestamp), {"host": host}, {"value": 1.0, "ok": True})
        for timestamp in (start, start + 1) for host in ("a", "b")
    ])

    first = changes(http, config, (format_timestamp(start - 1),))
    assert len(first) == 3
    assert all(doc.time == format_timestamp(start) for doc in first[:-1])
    assert [doc.tags for doc in first[:-1]] == [{"host": "a"}, {"host": "b"}]
    assert all(doc.fields == {"value": 1.0, "ok": True} for doc in first[:-1])
    assert first[-1] == (format_timestamp(start),)
    second = changes(http, config, first[-1])
    assert len(second) == 3
    assert all(doc.time == format_timestamp(start + 1) for doc in second[:-1])
    assert second[-1] == (format_timestamp(start + 1),)


@freeze_time("2026-11-10T15:00:00Z", real_asyncio=True)
def test_expired_measurement_does_not_stop_healthy_binding(http, config, monkeypatch, point_server):
    point_server.append(("readings", START, {}, {"value": 1.0}))
    records = capture_once(http, config, monkeypatch, names=("readings", "expired"))
    docs = [record["captured"] for record in records if "captured" in record]
    assert len(docs) == 1
    assert docs[0]["binding"] == 0
    assert docs[0]["doc"]["fields"] == {"value": 1.0}
    state = committed_state(records)["bindingStateV1"]
    assert state["readings"]["inc"]["cursor"] == [START]
    assert state["expired"]["inc"]["cursor"] == [format_timestamp(parse_timestamp(config.start_date) - 1)]


@pytest.mark.parametrize("interval", [timedelta(minutes=5), timedelta(seconds=17)])
def test_busy_binding_observes_interval(http, config, monkeypatch, point_server, interval):
    binding = CaptureBinding[ResourceConfig].model_validate({
        "resourceConfig": {"name": "sensors.readings", "interval": interval},
        "resourcePath": ["sensors.readings"], "stateKey": "readings",
        "collection": {"name": "test/readings", "key": ["/series", "/time"], "writeSchema": {}},
    })
    state = ResourceState(inc=ResourceState.Incremental(cursor=(STOP,)))
    opener = Mock()
    monkeypatch.setattr(common, "open_binding", opener)
    resources.open_binding(http, config, binding, 0, state, Mock(), [])
    fetch = opener.call_args.kwargs["fetch_changes"]
    sleeps = []

    with freeze_time("2026-11-10T15:00:00Z", real_asyncio=True) as frozen:
        async def run():
            task = Mock(log=LOG, stopping=Task.Stopping())

            async def checkpoint(*args):
                if task.checkpoint.await_count >= 3:
                    task.stopping.event.set()

            async def busy_fetch(log, cursor):
                point_server.append(("readings", format_timestamp(parse_timestamp(STOP) + len(point_server) * 1_000_000_000 + 1), {}, {"value": 1.0}))
                frozen.tick(timedelta(seconds=1))
                async for item in fetch(log, cursor):
                    yield item

            async def wait_for(awaitable, timeout):
                awaitable.close()
                if timeout == 0:
                    raise asyncio.TimeoutError
                sleeps.append(timeout)
                task.stopping.event.set()

            task.checkpoint = AsyncMock(side_effect=checkpoint)
            monkeypatch.setattr(common.asyncio, "wait_for", wait_for)
            await common._binding_incremental_task(binding, 0, busy_fetch, state.inc, task)
            assert task.checkpoint.await_count == 1

            frozen.tick(interval)
            resumed = [item async for item in fetch(LOG, state.inc.cursor)]
            assert len(resumed) == 2
            assert resumed[0].time == format_timestamp(parse_timestamp(STOP) + 1_000_000_001)

        asyncio.run(run())

    assert sleeps == [interval.total_seconds()]
    assert http.request_stream.await_count == 2


def test_omitted_interval_waits_after_lookback(http, config, monkeypatch, point_server):
    config.advanced.lookback_hours = 3
    binding = CaptureBinding[ResourceConfig].model_validate({
        "resourceConfig": ResourceConfig.model_validate({"name": "sensors.readings"}),
        "resourcePath": ["sensors.readings"], "stateKey": "readings",
        "collection": {"name": "test/readings", "key": ["/series", "/time"], "writeSchema": {}},
    })
    cursor = ("2026-11-10T14:00:00Z",)
    state = ResourceState(inc=ResourceState.Incremental(cursor=cursor))
    opener = Mock()
    monkeypatch.setattr(common, "open_binding", opener)
    resources.open_binding(http, config, binding, 0, state, Mock(), [])
    fetch = opener.call_args.kwargs["fetch_changes"]
    point_server.append(("readings", START, {}, {"value": 1.0}))

    with freeze_time("2026-11-10T15:00:00Z") as frozen:
        first = collect(fetch(LOG, cursor))
        assert [doc.time for doc in first[:-1]] == [START]
        assert first[-1] == cursor
        assert collect(fetch(LOG, cursor)) == []
        calls = http.request_stream.await_count
        assert collect(fetch(LOG, cursor)) == []
        assert http.request_stream.await_count == calls

        frozen.tick(timedelta(minutes=5) - timedelta(seconds=1))
        assert collect(fetch(LOG, cursor)) == []
        assert http.request_stream.await_count == calls
        frozen.tick(timedelta(seconds=1))
        replay = collect(fetch(LOG, cursor))
        assert [doc.time for doc in replay[:-1]] == [START]
        assert replay[-1] == cursor

    assert binding.resourceConfig.interval == timedelta(minutes=5)


@freeze_time("2026-11-10T15:00:00Z")
@pytest.mark.parametrize("timestamp", [START, "1969-12-31T23:59:59Z"])
def test_incremental_nanoseconds_and_shared_timestamps(http, config, point_server, timestamp):
    cursor = (timestamp,)
    first = format_timestamp(parse_timestamp(timestamp) + 1)
    second = format_timestamp(parse_timestamp(timestamp) + 2)
    point_server.extend([
        ("readings", timestamp, {"host": "old"}, {"value": 1.0}),
        ("readings", first, {"host": "a"}, {"value": 2.0, "ok": True}),
        ("readings", first, {"host": "b"}, {"value": 3.0, "ok": False}),
    ])

    emitted = changes(http, config, cursor)
    assert [(doc.time, doc.tags, doc.fields) for doc in emitted[:-1]] == [
        (first, {"host": "a"}, {"value": 2.0, "ok": True}),
        (first, {"host": "b"}, {"value": 3.0, "ok": False}),
    ]
    assert emitted[-1] == (first,)
    state = ResourceState.Incremental.model_validate_json(
        ResourceState.Incremental(cursor=emitted[-1]).model_dump_json()
    )
    assert changes(http, config, state.cursor) == []

    # Rewrites and late arrivals at/before the cursor are outside the contract.
    point_server.extend([
        ("readings", timestamp, {"host": "late"}, {"value": 10.0}),
        ("readings", first, {"host": "a"}, {"value": 20.0}),
        ("readings", second, {"host": "a"}, {"value": 4.0}),
    ])
    resumed = changes(http, config, state.cursor)
    assert len(resumed) == 2
    assert resumed[0].time == second
    assert resumed[0].fields == {"value": 4.0}
    assert resumed[-1] == (second,)


@freeze_time("2026-11-10T15:00:00Z")
def test_incremental_windows_resume_without_overlap(http, config, point_server):
    start = parse_timestamp("2026-09-01T00:00:00Z")
    boundary = start + int(WINDOW_SIZE.total_seconds()) * 1_000_000_000
    point_server.extend([
        ("readings", format_timestamp(start), {}, {"value": 1.0}),
        ("readings", format_timestamp(boundary - 1), {"host": "a"}, {"value": 2.0}),
        ("readings", format_timestamp(boundary - 1), {"host": "b"}, {"value": 3.0}),
        ("readings", format_timestamp(boundary), {}, {"value": 4.0}),
    ])
    first = changes(http, config, (format_timestamp(start - 1),))
    assert [doc.fields["value"] for doc in first[:-1]] == [1.0, 2.0, 3.0]
    assert first[-1] == (format_timestamp(boundary - 1),)
    state = ResourceState.Incremental.model_validate_json(
        ResourceState.Incremental(cursor=first[-1]).model_dump_json()
    )
    second = changes(http, config, state.cursor)
    assert len(second) == 2
    assert second[0].fields == {"value": 4.0}
    assert second[-1] == (format_timestamp(boundary),)


@freeze_time("2026-11-10T15:00:00Z")
@pytest.mark.parametrize("days", [1, 31, 90])
def test_empty_incremental_windows_preserve_cursor(http, config, point_server, days):
    start = datetime_to_ns(datetime.fromisoformat(STOP) - timedelta(days=days))
    cursor = (format_timestamp(start - 1),)
    assert changes(http, config, cursor) == []
    # An idle poll must not discard a later write between the cursor and cutoff.
    point_server.append(("readings", format_timestamp(start), {}, {"value": 1.0}))
    emitted = changes(http, config, cursor)
    assert len(emitted) == 2
    assert emitted[0].time == format_timestamp(start)
    assert emitted[-1] == (format_timestamp(start),)


@freeze_time("2026-11-10T15:00:00Z")
def test_incremental_crosses_empty_window_without_checkpoint(http, config, point_server):
    start = datetime_to_ns(datetime.fromisoformat(STOP) - WINDOW_SIZE * 2)
    timestamp = format_timestamp(start + int(WINDOW_SIZE.total_seconds()) * 1_000_000_000)
    point_server.append(("readings", timestamp, {}, {"value": 1.0}))
    emitted = changes(http, config, (format_timestamp(start - 1),))
    assert len(emitted) == 2
    assert emitted[0].time == timestamp
    assert emitted[-1] == (timestamp,)
    assert http.request_stream.await_count == 2


def test_incremental_lag_boundary(http, config, point_server):
    cutoff = parse_timestamp(STOP)
    point_server.extend([
        ("readings", format_timestamp(cutoff - 1), {}, {"value": 1.0}),
        ("readings", STOP, {}, {"value": 2.0}),
        ("readings", format_timestamp(cutoff + 1), {}, {"value": 3.0}),
        ("readings", "2026-11-10T15:00:00Z", {}, {"value": 4.0}),
    ])
    with freeze_time("2026-11-10T15:00:00Z") as frozen:
        first = changes(http, config, (format_timestamp(cutoff - 2),))
        assert len(first) == 2
        assert first[0].fields == {"value": 1.0}
        assert first[-1] == (format_timestamp(cutoff - 1),)
        frozen.tick(timedelta(seconds=1))
        second = changes(http, config, first[-1])
        assert [doc.fields["value"] for doc in second[:-1]] == [2.0, 3.0]
        assert second[-1] == (format_timestamp(cutoff + 1),)


@freeze_time("2026-11-10T15:00:00Z")
@pytest.mark.parametrize("offset", [-1, 0, 1])
def test_incremental_starts_at_or_after_cutoff(http, config, offset):
    assert changes(http, config, (format_timestamp(parse_timestamp(STOP) + offset),)) == []
    http.request_stream.assert_not_awaited()


@freeze_time("2026-11-10T15:00:00Z")
def test_incremental_stream_failure_does_not_checkpoint(http, config):
    async def body():
        yield table([["", "", "0", START, "1", "value", "readings", "a", "west"]])
        raise ConnectionError("made-up interrupted response")

    http.request_stream.side_effect = [
        stream(f",result,table,_time\n,,0,{START}\n".encode()), ({}, body),
    ]
    emitted = []

    async def run():
        async for item in fetch_points(
            http, config.url, config.org, config.common_headers, "sensors", "readings",
            config.advanced.lag_seconds, LOG, (format_timestamp(parse_timestamp(START) - 1),),
        ):
            emitted.append(item)

    with pytest.raises(ConnectionError):
        asyncio.run(run())
    assert emitted == []


def test_resume_preserves_first_default_start_date(http, config, monkeypatch, point_server):
    values = config.model_dump(exclude={"start_date"})
    with freeze_time("2026-11-10T15:00:00Z", real_asyncio=True):
        first_config = EndpointConfig.model_validate(values)
        first = capture_once(http, first_config, monkeypatch)
        assert not any("captured" in r for r in first)
        state = committed_state(first)
        expected_cursor = [format_timestamp(parse_timestamp(first_config.start_date) - 1)]
        assert state["bindingStateV1"]["readings"]["inc"]["cursor"] == expected_cursor

    with freeze_time("2026-11-11T15:00:00Z", real_asyncio=True):
        second_config = EndpointConfig.model_validate(values)
        assert second_config.start_date > first_config.start_date
        timestamp = format_timestamp(parse_timestamp(first_config.start_date))
        point_server.append(("readings", timestamp, {}, {"value": 1.0}))
        second = capture_once(http, second_config, monkeypatch, state)
        docs = [r["captured"]["doc"] for r in second if "captured" in r]
        assert len(docs) == 1
        assert docs[0]["time"] == timestamp


def test_default_start_date_keeps_lookback_after_restart(http, config, monkeypatch, point_server):
    config.advanced.lookback_hours = 2
    values = config.model_dump(exclude={"start_date"})
    cursor = "2026-10-12T12:00:00Z"
    late = "2026-10-12T11:00:00Z"
    point_server.append(("readings", cursor, {}, {"value": 1.0}))

    with freeze_time("2026-11-10T15:00:00Z", real_asyncio=True):
        first_config = EndpointConfig.model_validate(values)
        first = capture_once(http, first_config, monkeypatch, names=("readings",), finish_sweep=True)
        assert [r["captured"]["doc"]["time"] for r in first if "captured" in r] == [cursor]
        state = committed_state(first)
        assert state["bindingStateV1"]["readings"]["inc"]["cursor"] == [cursor]

    point_server[:] = [
        ("readings", late, {}, {"value": 2.0}),
        ("readings", cursor, {}, {"value": 3.0, "added": True}),
    ]
    with freeze_time("2026-11-20T15:00:00Z", real_asyncio=True):
        second_config = EndpointConfig.model_validate(values)
        assert second_config.start_date > cursor > first_config.start_date
        resumed = capture_once(http, second_config, monkeypatch, state, names=("readings",), finish_sweep=True)
        docs = [r["captured"]["doc"] for r in resumed if "captured" in r]
        assert [(doc["time"], doc["fields"]) for doc in docs] == [
            (late, {"value": 2.0}), (cursor, {"value": 3.0, "added": True}),
        ]
        assert committed_state(resumed, state) == state


@freeze_time("2026-11-10T15:00:00Z", real_asyncio=True)
def test_lookback_resumes_checkpoints_and_merges_late_points(http, config, monkeypatch, point_server):
    config.advanced.lookback_hours = 2
    newest = "2026-11-10T14:00:00.000000001Z"
    boundary = "2026-11-10T12:00:00.000000001Z"
    point_server.append(("readings", newest, {"host": "a"}, {"value": 1.0}))
    first = capture_once(http, config, monkeypatch, names=("readings",), finish_sweep=True)
    state = committed_state(first)
    assert state["bindingStateV1"]["readings"]["inc"]["cursor"] == [newest]

    point_server[:] = [
        ("readings", newest, {"host": "a"}, {"value": 2.0, "added": True}),
        ("readings", boundary, {"host": "late"}, {"value": 3.0}),
        ("readings", START, {"host": "too-old"}, {"value": 4.0}),
        ("readings", STOP, {"host": "too-new"}, {"value": 5.0}),
    ]
    resumed = capture_once(http, config, monkeypatch, state, names=("readings",), finish_sweep=True)
    docs = [r["captured"]["doc"] for r in resumed if "captured" in r]
    assert [(doc["time"], doc["fields"]) for doc in docs] == [
        (boundary, {"value": 3.0}), (newest, {"value": 2.0, "added": True}),
    ]
    cursors = [
        r["checkpoint"]["state"]["updated"]["bindingStateV1"]["readings"]["inc"]["cursor"]
        for r in resumed if "checkpoint" in r
    ]
    assert cursors == [[newest], [newest]]
    assert committed_state(resumed, state) == state
    merged = {
        (r["captured"]["doc"]["series"], r["captured"]["doc"]["time"]): r["captured"]["doc"]
        for r in first + resumed if "captured" in r
    }
    assert len(merged) == 2
    assert merged[("readings,host=a", newest)]["fields"] == {"value": 2.0, "added": True}


def test_lookback_drains_windows_once_then_observes_interval(http, config, point_server):
    config.advanced.lookback_hours = 3
    cursor = ("2026-11-10T14:00:00Z",)
    point_server.extend([
        ("readings", "2026-11-10T11:00:00Z", {}, {"value": 1.0}),
        ("readings", START, {}, {"value": 2.0}),
        ("readings", "2026-11-10T14:30:00Z", {}, {"value": 3.0}),
    ])
    state = api.FetchState()
    with freeze_time("2026-11-10T15:00:00Z") as frozen:
        first = changes(http, config, cursor, fetch_state=state)
        assert first[-1] == cursor
        assert [doc.time for doc in first[:-1]] == ["2026-11-10T11:00:00Z"]
        second = changes(http, config, first[-1], fetch_state=state)
        assert second[-1] == cursor
        assert [doc.time for doc in second[:-1]] == [START]
        third = changes(http, config, second[-1], fetch_state=state)
        assert third[-1] == ("2026-11-10T14:30:00Z",)
        assert [doc.fields for doc in third[:-1]] == [{"value": 3.0}]
        calls = http.request_stream.await_count
        assert changes(http, config, third[-1], fetch_state=state) == []
        assert http.request_stream.await_count == calls

        frozen.tick(timedelta(minutes=5))
        replay = changes(http, config, third[-1], fetch_state=state)
        assert [doc.time for doc in replay[:-1]] == [START]
        assert replay[-1] == third[-1]


@freeze_time("2026-11-10T15:00:00Z", real_asyncio=True)
def test_lookback_interrupted_replay_restarts_without_skips(http, config, monkeypatch, point_server):
    config.advanced.lookback_hours = 3
    cursor = "2026-11-10T14:00:00Z"
    point_server.append(("readings", cursor, {}, {"value": 1.0}))
    first = capture_once(http, config, monkeypatch, names=("readings",))
    state = committed_state(first)
    point_server.extend([
        ("readings", START, {}, {"value": 2.0}),
        ("readings", "2026-11-10T14:30:00Z", {}, {"value": 3.0}),
    ])
    interrupted = capture_once(http, config, monkeypatch, state, names=("readings",))
    interrupted_state = committed_state(interrupted, state)
    assert interrupted_state["bindingStateV1"]["readings"]["inc"]["cursor"] == [cursor]
    assert [r["captured"]["doc"]["time"] for r in interrupted if "captured" in r] == [START]

    resumed = capture_once(http, config, monkeypatch, interrupted_state, names=("readings",), finish_sweep=True)
    assert [r["captured"]["doc"]["time"] for r in resumed if "captured" in r] == [
        START, cursor, "2026-11-10T14:30:00Z",
    ]
    assert committed_state(resumed, interrupted_state)["bindingStateV1"]["readings"]["inc"]["cursor"] == ["2026-11-10T14:30:00Z"]


@freeze_time("2026-11-10T15:00:00Z")
def test_lookback_respects_start_date_and_empty_polls(http, config, point_server):
    config.advanced.lookback_hours = 24
    config.start_date = START
    cursor = (format_timestamp(parse_timestamp(START) - 1),)
    state = api.FetchState()
    assert changes(http, config, cursor, fetch_state=state) == []
    assert state.next_start is state.sweep_end is None
    point_server.extend([
        ("readings", "2026-11-10T11:59:59.999999999Z", {}, {"value": 1.0}),
        ("readings", START, {}, {"value": 2.0}),
    ])
    replay = changes(http, config, cursor)
    assert [doc.time for doc in replay[:-1]] == [START]
    assert replay[-1] == (START,)


@freeze_time("2026-11-10T15:00:00Z")
def test_default_poll_rereads_uncaptured_window_tail(http, config, point_server):
    point_server.append(("readings", START, {}, {"value": 1.0}))
    state = api.FetchState()
    first = changes(http, config, (format_timestamp(parse_timestamp(START) - 1),), fetch_state=state)
    assert first[-1] == (START,)
    point_server.append(("readings", "2026-11-10T12:30:00Z", {}, {"value": 2.0}))
    resumed = changes(http, config, first[-1], fetch_state=state)
    assert [doc.time for doc in resumed[:-1]] == ["2026-11-10T12:30:00Z"]
    assert resumed[-1] == ("2026-11-10T12:30:00Z",)


@freeze_time("2026-11-10T15:00:00Z", real_asyncio=True)
def test_capture_dotted_measurement_from_explicit_components(http, config, monkeypatch, point_server):
    name = "app.requests"
    point_server.append((name, START, {"host": "a"}, {"value": 1.0}))
    records = capture_once(http, config, monkeypatch, names=(name,), resource_configs={
        name: {"name": "sensors%2Ewest/app%2Erequests", "bucket": "sensors.west", "measurement": name, "interval": "PT5M"},
    })
    docs = [r["captured"]["doc"] for r in records if "captured" in r]
    assert len(docs) == 1
    assert docs[0]["measurement"] == name
    assert docs[0]["series"] == "app.requests,host=a"
    assert docs[0]["fields"] == {"value": 1.0}
    assert all('from(bucket: "sensors.west")' in call.kwargs["json"]["query"] for call in http.request_stream.await_args_list)
    state = committed_state(records)
    assert state["bindingStateV1"][name]["inc"]["cursor"] == [START]
    resumed = capture_once(http, config, monkeypatch, state, names=(name,), resource_configs={
        name: {"name": "sensors%2Ewest/app%2Erequests", "bucket": "sensors.west", "measurement": name, "interval": "PT5M"},
    })
    assert not any("captured" in r for r in resumed)
