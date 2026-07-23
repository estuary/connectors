import json
import socket
import threading
import time

import pytest

from snowpipe_sidecar.channels import ChannelManager
from snowpipe_sidecar.server import Server


class StubStatus:
    def __init__(self, token):
        self.latest_committed_offset_token = token
        self.rows_inserted_count = 0
        self.rows_error_count = 0
        self.last_error_message = ""


class StubChannel:
    """In-memory stand-in for the SDK channel: appends commit instantly."""

    def __init__(self, client, name):
        self.client = client
        self.name = name
        self.committed = client.committed.get(name)
        self.closed = False

    def append_rows(self, rows, start_token, end_token):
        assert start_token == end_token
        for row in rows:
            if not isinstance(row, dict):
                raise TypeError(f"row is not a dict: {row!r}")
        self.committed = end_token
        self.client.committed[self.name] = end_token

    def wait_for_commit(self, checker, timeout_seconds=None):
        if not checker(self.committed):
            raise RuntimeError(f"not committed at {self.committed!r}")

    def get_latest_committed_offset_token(self):
        return self.committed

    def get_channel_status(self):
        return StubStatus(self.committed)

    def close(self):
        self.closed = True


class StubClient:
    def __init__(self):
        self.committed = {}
        self.closed = False

    def open_channel(self, name):
        ch = StubChannel(self, name)
        return ch, StubStatus(self.committed.get(name))

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

    def call(self, op, **params):
        self.next_id += 1
        line = json.dumps({"id": self.next_id, "op": op, "params": params}) + "\n"
        self.client_sock.sendall(line.encode())
        res = json.loads(self.rfile.readline())
        assert res["id"] == self.next_id
        return res


@pytest.fixture
def harness():
    return Harness()


def configure(harness, auth="secret-token"):
    return harness.call("configure", profile={"account": "a", "user": "u", "url": "https://x", "private_key": "k"}, auth=auth)


def test_configure_and_full_flow(harness):
    assert configure(harness)["ok"]

    res = harness.call("open_channel", database="DB", schema="SCH", table="TBL", channel="ch_0")
    assert res["ok"] and res["result"]["committed_token"] is None

    res = harness.call("append", channel="ch_0", token="base:0", rows=[{"ID": 1}, {"ID": 2}])
    assert res["ok"] and res["result"]["appended"] == 2

    res = harness.call("wait_commit", channel="ch_0", token="base:0", timeout_s=5)
    assert res["ok"] and res["result"]["committed_token"] == "base:0"

    res = harness.call("channel_status", channel="ch_0")
    assert res["ok"] and res["result"]["committed_token"] == "base:0"

    # Reopen reports the committed token.
    res = harness.call("open_channel", database="DB", schema="SCH", table="TBL", channel="ch_0")
    assert res["ok"] and res["result"]["committed_token"] == "base:0"


def test_bad_auth(harness):
    res = configure(harness, auth="wrong")
    assert not res["ok"] and res["code"] == "auth"


def test_ops_before_configure(harness):
    res = harness.call("open_channel", database="DB", schema="SCH", table="TBL", channel="ch_0")
    assert not res["ok"] and res["code"] == "protocol"


def test_unknown_channel(harness):
    assert configure(harness)["ok"]
    res = harness.call("append", channel="nope", token="t", rows=[])
    assert not res["ok"] and res["code"] == "unknown_channel"


def test_unknown_op(harness):
    assert configure(harness)["ok"]
    res = harness.call("frobnicate")
    assert not res["ok"] and res["code"] == "protocol"


def test_invalid_rows(harness):
    assert configure(harness)["ok"]
    assert harness.call("open_channel", database="DB", schema="SCH", table="TBL", channel="ch_0")["ok"]
    res = harness.call("append", channel="ch_0", token="t", rows=["not-a-dict"])
    assert not res["ok"] and res["code"] == "invalid_rows"


def test_channels_are_independent(harness):
    assert configure(harness)["ok"]
    assert harness.call("open_channel", database="DB", schema="SCH", table="T1", channel="ch_1")["ok"]
    assert harness.call("open_channel", database="DB", schema="SCH", table="T2", channel="ch_2")["ok"]

    assert harness.call("append", channel="ch_1", token="a:0", rows=[{"ID": 1}])["ok"]
    assert harness.call("append", channel="ch_2", token="b:0", rows=[{"ID": 2}])["ok"]

    assert harness.call("wait_commit", channel="ch_1", token="a:0", timeout_s=5)["result"]["committed_token"] == "a:0"
    assert harness.call("wait_commit", channel="ch_2", token="b:0", timeout_s=5)["result"]["committed_token"] == "b:0"


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
    res = harness.call("append", channel="ch_0", token="t:0", rows=[{"ID": 1}])
    assert res["ok"] and res["result"]["appended"] == 1
    assert remaining["n"] == 0

    # Persistent saturation past the deadline surfaces as a classified error.
    monkeypatch.setattr(ch_mod, "_SATURATION_MAX_WAIT_S", 0.02)
    remaining["n"] = 10_000
    res = harness.call("append", channel="ch_0", token="t:1", rows=[{"ID": 2}])
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
