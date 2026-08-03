import json
import socket
import threading
import time

import msgspec
import pytest
from msgspec import Raw

from snowpipe_sidecar.channels import ChannelManager
from snowpipe_sidecar.server import Server


class StubStatus:
    def __init__(self, token, rows_error_count=0, last_error_message=""):
        self.latest_committed_offset_token = token
        self.rows_inserted_count = 0
        self.rows_error_count = rows_error_count
        self.last_error_message = last_error_message


class StubChannel:
    """In-memory stand-in for the SDK channel: appends commit instantly."""

    def __init__(self, client, name):
        self.client = client
        self.name = name
        self.committed = client.committed.get(name)
        self.closed = False

    def _status(self):
        return StubStatus(
            self.committed,
            self.client.rows_error_count.get(self.name, 0),
            self.client.last_error_message.get(self.name, ""),
        )

    def append_rows(self, rows, start_token, end_token):
        self.client.spans.append((start_token, end_token))
        # The SDK's own encoder is what turns these into the NDJSON block its
        # native core wants, so what the sidecar owes it is one pre-encoded row
        # per row and nothing else. A dict here would be re-encoded, which is
        # the round trip this path exists to avoid; anything else would be
        # ingested as the wrong thing entirely.
        for row in rows:
            if not isinstance(row, Raw):
                raise TypeError(f"row is not a msgspec.Raw: {row!r}")
        self.client.appended.setdefault(self.name, []).extend(bytes(row) for row in rows)
        self.committed = end_token
        self.client.committed[self.name] = end_token

    def wait_for_commit(self, checker, timeout_seconds=None):
        if not checker(self.committed):
            raise RuntimeError(f"not committed at {self.committed!r}")

    def get_latest_committed_offset_token(self):
        return self.committed

    def get_channel_status(self):
        return self._status()

    def close(self, drop=False, wait_for_flush=True, timeout_seconds=None):
        self.closed = True
        if drop:
            # Only a drop reaches Snowflake, where it takes the channel's
            # committed offset token and row-error statistics with it.
            self.client.committed.pop(self.name, None)
            self.client.rows_error_count.pop(self.name, None)
            self.client.last_error_message.pop(self.name, None)


class StubClient:
    def __init__(self):
        self.committed = {}
        self.rows_error_count = {}
        self.last_error_message = {}
        self.spans = []
        self.appended = {}
        self.closed = False

    def open_channel(self, name):
        ch = StubChannel(self, name)
        return ch, ch._status()

    def close(self):
        self.closed = True


class Harness:
    """Runs a Server over a socketpair and provides synchronous RPC calls."""

    def __init__(self):
        self.client_sock, server_sock = socket.socketpair()
        self.exit_codes = []
        self.stub_clients = {}

        def client_factory(props, database, schema, table):
            client = self.stub_clients.setdefault((database, schema, table), StubClient())
            client.props = props
            return client

        self.server = Server(
            server_sock,
            auth_token="secret-token",
            manager_factory=lambda profile: ChannelManager(profile, client_factory),
            exit_fn=self.exit_codes.append,
        )
        self.thread = threading.Thread(target=self.server.serve, daemon=True)
        self.thread.start()
        self.rfile = self.client_sock.makefile("r", encoding="utf-8")
        self.next_id = 0

    def call(self, op, payload=None, **params):
        self.next_id += 1
        line = json.dumps({"id": self.next_id, "op": op, "params": params}) + "\n"
        self.client_sock.sendall(line.encode() if payload is None else line.encode() + payload)
        res = json.loads(self.rfile.readline())
        assert res["id"] == self.next_id
        return res

    def append(self, channel, start_token, end_token, rows=None, payload=None, row_count=None):
        """Append rows through the framing the connector uses: a header line
        declaring the batch, and the rows themselves as the payload which
        follows it. Giving the payload or the row count outright is how a test
        sends what a well-behaved connector never would."""
        if payload is None:
            payload = raw_payload(rows)
        if row_count is None:
            row_count = len(rows)
        return self.call(
            "append",
            payload=payload,
            channel=channel,
            start_token=start_token,
            end_token=end_token,
            row_count=row_count,
            payload_len=len(payload),
        )


    def send_raw(self, op, payload=b"", **params):
        """Send a request the reader is not expected to reply to."""
        self.next_id += 1
        line = json.dumps({"id": self.next_id, "op": op, "params": params}) + "\n"
        self.client_sock.sendall(line.encode() + payload)

    def wait_for_exit(self):
        for _ in range(200):
            if self.exit_codes:
                return self.exit_codes
            time.sleep(0.005)
        return self.exit_codes


def raw_payload(rows):
    encoder = msgspec.json.Encoder()
    return b"[" + b",".join(encoder.encode(row) for row in rows) + b"]"


@pytest.fixture
def harness():
    return Harness()


def configure(harness, auth="secret-token"):
    return harness.call("configure", profile={"account": "a", "user": "u", "url": "https://x", "private_key": "k"}, auth=auth)


def test_configure_and_full_flow(harness):
    assert configure(harness)["ok"]

    res = harness.call("open_channel", database="DB", schema="SCH", table="TBL", channel="ch_0")
    assert res["ok"] and res["result"]["committed_token"] is None

    res = harness.append("ch_0", "base:0", "base:0", [{"ID": 1}, {"ID": 2}])
    assert res["ok"] and res["result"]["appended"] == 2

    res = harness.call("wait_commit", channel="ch_0", token="base:0", timeout_s=5)
    assert res["ok"] and res["result"]["committed_token"] == "base:0"

    res = harness.call("channel_status", channel="ch_0")
    assert res["ok"] and res["result"]["committed_token"] == "base:0"

    # Reopen reports the committed token.
    res = harness.call("open_channel", database="DB", schema="SCH", table="TBL", channel="ch_0")
    assert res["ok"] and res["result"]["committed_token"] == "base:0"


def test_append_spans_a_token_range(harness):
    # A batch covers the offset tokens of its first and last row, and the
    # channel's committed token advances to the last.
    assert configure(harness)["ok"]
    assert harness.call("open_channel", database="DB", schema="SCH", table="TBL", channel="ch_0")["ok"]

    res = harness.append("ch_0", "1", "3", [{"ID": 1}, {"ID": 2}, {"ID": 3}])
    assert res["ok"] and res["result"]["appended"] == 3

    assert harness.stub_clients[("DB", "SCH", "TBL")].spans == [("1", "3")]
    assert harness.call("channel_status", channel="ch_0")["result"]["committed_token"] == "3"


def test_row_error_statistics_accompany_open_and_commit(harness):
    # Snowflake reports a rejected row only in the channel's row-error
    # statistics, so both the reply that establishes a baseline for a channel
    # (open_channel) and the reply the transaction blocks on (wait_commit) must
    # carry them.
    assert configure(harness)["ok"]
    assert harness.call("open_channel", database="DB", schema="SCH", table="TBL", channel="ch_0")["ok"]

    stub = harness.stub_clients[("DB", "SCH", "TBL")]
    stub.rows_error_count["ch_0"] = 3
    stub.last_error_message["ch_0"] = "NULL result in a non-nullable column"

    assert harness.append("ch_0", "1", "2", [{"ID": 1}, {"ID": 2}])["ok"]

    res = harness.call("wait_commit", channel="ch_0", token="2", timeout_s=5)
    assert res["ok"]
    assert res["result"] == {
        "committed_token": "2",
        "rows_error_count": 3,
        "last_error_message": "NULL result in a non-nullable column",
    }

    res = harness.call("open_channel", database="DB", schema="SCH", table="TBL", channel="ch_0")
    assert res["ok"]
    assert res["result"] == {
        "committed_token": "2",
        "rows_error_count": 3,
        "last_error_message": "NULL result in a non-nullable column",
    }


def test_bad_auth(harness):
    res = configure(harness, auth="wrong")
    assert not res["ok"] and res["code"] == "auth"


def test_ops_before_configure(harness):
    res = harness.call("open_channel", database="DB", schema="SCH", table="TBL", channel="ch_0")
    assert not res["ok"] and res["code"] == "protocol"


def test_unknown_channel(harness):
    assert configure(harness)["ok"]
    res = harness.append("nope", "t", "t", [{"ID": 1}])
    assert not res["ok"] and res["code"] == "unknown_channel"

    # A refused append's payload is still consumed, or what remained of it would
    # be read as the header of whatever came next.
    assert harness.call("open_channel", database="DB", schema="SCH", table="TBL", channel="ch_0")["ok"]


def test_unknown_op(harness):
    assert configure(harness)["ok"]
    res = harness.call("frobnicate")
    assert not res["ok"] and res["code"] == "protocol"


def test_rows_reach_the_sdk_as_the_payload_carried_them(harness):
    # The point of the framing: a row's bytes are handed to the SDK exactly as
    # the connector encoded them, never having been a Python object here.
    assert configure(harness)["ok"]
    assert harness.call("open_channel", database="DB", schema="SCH", table="TBL", channel="ch_0")["ok"]

    encoded = [
        '{"ID":1,"DOC":{"nested":["a",1,null,{"b":true}]}}'.encode(),
        '{"ID":2,"S":"你好 🌟"}'.encode(),
    ]
    res = harness.append("ch_0", "1", "2", payload=b"[" + b",".join(encoded) + b"]", row_count=2)
    assert res["ok"] and res["result"]["appended"] == 2

    assert harness.stub_clients[("DB", "SCH", "TBL")].appended["ch_0"] == encoded


def test_invalid_rows(harness):
    assert configure(harness)["ok"]
    assert harness.call("open_channel", database="DB", schema="SCH", table="TBL", channel="ch_0")["ok"]

    # Malformed row JSON: the scan which locates each row's extent is what
    # catches this, since nothing downstream would — the SDK's encoder writes a
    # row out verbatim.
    res = harness.append("ch_0", "t", "t", payload=b'[{"ID":1},{oops}]', row_count=2)
    assert not res["ok"] and res["code"] == "invalid_rows"

    # A row which is valid JSON but not an object. The scan accepts it, so it
    # takes a check of its own; left alone it would reach Snowflake, which
    # discards what it cannot store as a rejected row rather than failing.
    res = harness.append("ch_0", "t", "t", ["not-an-object"])
    assert not res["ok"] and res["code"] == "invalid_rows"
    res = harness.append("ch_0", "t", "t", [{"ID": 1}, [{"ID": 2}]])
    assert not res["ok"] and res["code"] == "invalid_rows"

    # A payload holding a different number of rows than the header declares: the
    # count is what the offset tokens of the batch account for, so a mismatch
    # cannot be committed under them.
    res = harness.append("ch_0", "t", "t", [{"ID": 1}, {"ID": 2}], row_count=3)
    assert not res["ok"] and res["code"] == "invalid_rows"

    # Every refusal above leaves the channel usable, since none of them was the
    # framing losing its place in the stream.
    assert harness.append("ch_0", "1", "1", [{"ID": 1}])["ok"]


def test_append_framing_failures_are_fatal(harness):
    # A payload shorter than its header declared, or a header which declares no
    # payload at all, leaves the reader unable to say where the next request
    # begins. There is nothing to resynchronize on, so the sidecar exits and lets
    # the connector's supervision restart it.
    assert configure(harness)["ok"]
    assert harness.call("open_channel", database="DB", schema="SCH", table="TBL", channel="ch_0")["ok"]

    harness.send_raw(
        "append",
        payload=b'[{"ID":1}]',
        channel="ch_0",
        start_token="1",
        end_token="1",
        row_count=1,
        payload_len=1024,
    )
    # EOF, so the payload the header promised can never arrive.
    harness.client_sock.shutdown(socket.SHUT_WR)
    assert harness.wait_for_exit() == [3]


def test_append_without_a_payload_length_is_fatal(harness):
    assert configure(harness)["ok"]
    assert harness.call("open_channel", database="DB", schema="SCH", table="TBL", channel="ch_0")["ok"]

    harness.send_raw("append", channel="ch_0", start_token="1", end_token="1", row_count=1)
    assert harness.wait_for_exit() == [3]


def test_channels_are_independent(harness):
    assert configure(harness)["ok"]
    assert harness.call("open_channel", database="DB", schema="SCH", table="T1", channel="ch_1")["ok"]
    assert harness.call("open_channel", database="DB", schema="SCH", table="T2", channel="ch_2")["ok"]

    assert harness.append("ch_1", "a:0", "a:0", [{"ID": 1}])["ok"]
    assert harness.append("ch_2", "b:0", "b:0", [{"ID": 2}])["ok"]

    assert harness.call("wait_commit", channel="ch_1", token="a:0", timeout_s=5)["result"]["committed_token"] == "a:0"
    assert harness.call("wait_commit", channel="ch_2", token="b:0", timeout_s=5)["result"]["committed_token"] == "b:0"


def test_close_channel_releases_the_handle(harness):
    assert configure(harness)["ok"]
    assert harness.call("open_channel", database="DB", schema="SCH", table="TBL", channel="ch_0")["ok"]
    assert harness.append("ch_0", "1", "2", [{"ID": 1}, {"ID": 2}])["ok"]

    assert harness.call("close_channel", channel="ch_0")["ok"]
    stub = harness.stub_clients[("DB", "SCH", "TBL")]

    # The handle is spent, so an op which needs one is refused rather than
    # silently working against a closed channel.
    res = harness.append("ch_0", "3", "3", [{"ID": 3}])
    assert not res["ok"] and res["code"] == "unknown_channel"

    # Snowflake still holds the channel, so reopening it recovers the token.
    res = harness.call("open_channel", database="DB", schema="SCH", table="TBL", channel="ch_0")
    assert res["ok"] and res["result"]["committed_token"] == "2"
    assert stub.committed["ch_0"] == "2"


def test_close_channel_with_drop_retires_the_channel(harness):
    # Dropping is what retires a channel: the name reopens as one which has
    # committed nothing, so a later shard deriving it inherits no skip threshold.
    assert configure(harness)["ok"]
    assert harness.call("open_channel", database="DB", schema="SCH", table="TBL", channel="ch_0")["ok"]
    assert harness.append("ch_0", "1", "2", [{"ID": 1}, {"ID": 2}])["ok"]

    assert harness.call("close_channel", channel="ch_0", drop=True)["ok"]

    res = harness.call("open_channel", database="DB", schema="SCH", table="TBL", channel="ch_0")
    assert res["ok"] and res["result"]["committed_token"] is None


def test_close_unopened_channel(harness):
    assert configure(harness)["ok"]
    res = harness.call("close_channel", channel="nope", drop=True)
    assert not res["ok"] and res["code"] == "unknown_channel"


class _FakeErrorCode:
    def __init__(self, name):
        self.name = name


class _SaturationError(Exception):
    def __init__(self):
        super().__init__("ReceiverSaturated: buffer full")
        self.error_code = _FakeErrorCode("RECEIVER_SATURATED")


def test_receiver_saturated_retries(harness, monkeypatch):
    from snowpipe_sidecar import channels as ch_mod

    monkeypatch.setattr(ch_mod, "_SATURATION_INITIAL_DELAY_S", 0.01)
    monkeypatch.setattr(ch_mod, "_SATURATION_MAX_DELAY_S", 0.01)

    assert configure(harness)["ok"]
    assert harness.call("open_channel", database="DB", schema="SCH", table="TBL", channel="ch_0")["ok"]

    stub = harness.stub_clients[("DB", "SCH", "TBL")]
    orig = StubChannel.append_rows
    remaining = {"n": 3}

    def flaky(self, rows, start, end):
        if remaining["n"] > 0:
            remaining["n"] -= 1
            raise _SaturationError()
        return orig(self, rows, start, end)

    monkeypatch.setattr(StubChannel, "append_rows", flaky)
    res = harness.append("ch_0", "t:0", "t:0", [{"ID": 1}])
    assert res["ok"] and res["result"]["appended"] == 1
    assert remaining["n"] == 0

    # Persistent saturation past the deadline surfaces as a classified error.
    monkeypatch.setattr(ch_mod, "_SATURATION_MAX_WAIT_S", 0.02)
    remaining["n"] = 10_000
    res = harness.append("ch_0", "t:1", "t:1", [{"ID": 2}])
    assert not res["ok"] and res["code"] == "RECEIVER_SATURATED"


def test_shutdown_closes_everything(harness):
    assert configure(harness)["ok"]
    assert harness.call("open_channel", database="DB", schema="SCH", table="TBL", channel="ch_0")["ok"]

    res = harness.call("shutdown")
    assert res["ok"]
    # The exit callback runs on the control worker just after the reply is
    # written, so allow it a moment to fire.
    for _ in range(200):
        if harness.exit_codes:
            break
        time.sleep(0.005)
    assert harness.exit_codes == [0]
    for client in harness.stub_clients.values():
        assert client.closed
