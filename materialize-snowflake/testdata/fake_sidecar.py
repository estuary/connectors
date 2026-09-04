"""A stdlib-only fake of the snowpipe streaming sidecar, for Go unit tests.

Speaks the same readiness handshake and NDJSON RPC protocol as the real
sidecar — including an append's framing, whose rows follow the request line as a
payload of the length that line declares — backed by an in-memory "Snowflake"
that commits appends instantly.

Fault injection via the FAKE_SIDECAR_MODE environment variable:
  never_ready           never prints the ready line
  exit_before_ready     exits 7 before printing the ready line
  crash_after_configure prints garbage to stderr and exits 9 after configure
  hang_on_append        accepts appends but never responds to them
  commit_never_lands    accepts appends but never advances the committed token
  garbage_stderr        emits non-JSON stderr noise alongside normal operation
  ignore_sigterm        ignores SIGTERM so only SIGKILL can end it

A row carrying the REJECT column stands in for a row Snowflake's ingestion
discards: the append and the commit both succeed and the committed token still
advances over it, but the channel's row-error count moves. The count a channel
starts out with — as one which accumulated errors before this session would —
is seeded by FAKE_SIDECAR_ROWS_ERROR_COUNT.

FAKE_SIDECAR_STATE names a JSON file to keep channel state in, which is what
lets this fake's "Snowflake" outlive a sidecar process: a channel reopened by a
later session reports the committed offset token an earlier one left it with,
and a channel dropped by one session is gone for all of them. Unset, state lives
only as long as the process does.
"""

import json
import os
import signal
import socket
import sys
import threading

MODE = os.environ.get("FAKE_SIDECAR_MODE", "")
STATE_PATH = os.environ.get("FAKE_SIDECAR_STATE", "")


def log(level, msg, **fields):
    sys.stderr.write(json.dumps({"level": level, "msg": msg, "fields": fields}) + "\n")
    sys.stderr.flush()


def main():
    argv = sys.argv[1:]

    if MODE == "ignore_sigterm":
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    if MODE == "never_ready":
        signal.pause()
    if MODE == "exit_before_ready":
        sys.stderr.write("boom: fake sidecar failed to initialize\n")
        sys.stderr.flush()
        sys.exit(7)

    auth = sys.stdin.readline().strip()

    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(argv[argv.index("--uds") + 1])
    server.listen(1)
    sys.stdout.write(json.dumps({"ready": True}) + "\n")
    sys.stdout.flush()

    conn, _ = server.accept()
    # Binary, as the real sidecar's reader is: an append's rows follow its header
    # line as a payload read by exact length, which cannot be mixed with
    # text-mode reads of the same connection.
    rfile = conn.makefile("rb")
    wlock = threading.Lock()

    if MODE == "garbage_stderr":
        sys.stderr.write("2026-07-17T00:00:00Z | WARN | core (1) | native noise, not JSON\n")
        sys.stderr.write('{"level":"warning","msg":"structured sidecar warning","fields":{"source":"fake"}}\n')
        sys.stderr.flush()

    state = {"committed": {}, "errors": {}}  # channel -> token / rejected row count
    if STATE_PATH and os.path.exists(STATE_PATH):
        with open(STATE_PATH) as f:
            state = json.load(f)
    committed, errors = state["committed"], state["errors"]
    seeded_errors = int(os.environ.get("FAKE_SIDECAR_ROWS_ERROR_COUNT", "0"))
    configured = False

    def persist():
        if not STATE_PATH:
            return
        # Renamed into place so that a reader never sees a half-written file.
        # Sessions are assumed not to overlap on one state file, since each holds
        # the whole of it in memory and writes it back whole.
        tmp = STATE_PATH + f".{os.getpid()}.tmp"
        with open(tmp, "w") as f:
            json.dump({"committed": committed, "errors": errors}, f)
        os.replace(tmp, STATE_PATH)

    def reply(res):
        with wlock:
            conn.sendall((json.dumps(res) + "\n").encode())

    # Channels this session holds a handle for. The real sidecar refuses a
    # channel op it has no handle for — whether because the channel was never
    # opened or because it has since been closed — and a fake which accepted
    # them would let a caller pass tests the sidecar would reject.
    opened = set()

    def require_open(rid, ch):
        if ch in opened:
            return True
        reply({"id": rid, "ok": False, "error": f"channel {ch!r} is not open", "code": "unknown_channel"})
        return False

    def rejected_count(ch):
        return errors.get(ch, seeded_errors)

    def status(ch):
        count = rejected_count(ch)
        return {
            "committed_token": committed.get(ch),
            "rows_error_count": count,
            "last_error_message": "fake rejection" if count else "",
        }

    for line in rfile:
        req = json.loads(line)
        rid, op = req["id"], req["op"]
        params = req.get("params") or {}

        # An append's payload is consumed whatever becomes of the request, since
        # anything left of it would be read as the next request's header. The real
        # sidecar hands these bytes to the SDK unparsed; a stdlib fake has to
        # parse them to see the rows, which is only cheap because it is a fake.
        rows = None
        if op == "append":
            payload = rfile.read(params["payload_len"])
            if len(payload) != params["payload_len"]:
                log("error", "append payload truncated")
                os._exit(3)
            rows = json.loads(payload)
            if len(rows) != params["row_count"]:
                reply({"id": rid, "ok": False, "error": f"declared {params['row_count']} rows, payload holds {len(rows)}", "code": "invalid_rows"})
                continue
            elif any(not isinstance(row, dict) for row in rows):
                reply({"id": rid, "ok": False, "error": "row is not a JSON object", "code": "invalid_rows"})
                continue

        if op == "configure":
            if params.get("auth") != auth:
                reply({"id": rid, "ok": False, "error": "bad auth token", "code": "auth"})
                continue
            configured = True
            log("info", "fake sidecar configured")
            reply({"id": rid, "ok": True})
            if MODE == "crash_after_configure":
                sys.stderr.write("thread panicked at src/lib.rs:1:1: fake native crash\n")
                sys.stderr.flush()
                os._exit(9)
        elif not configured:
            reply({"id": rid, "ok": False, "error": "not configured", "code": "protocol"})
        elif op == "open_channel":
            ch = params["channel"]
            if params["table"].startswith("UNSUPPORTED"):
                reply({"id": rid, "ok": False, "error": "table not supported", "code": "unsupported_table"})
                continue
            opened.add(ch)
            reply({"id": rid, "ok": True, "result": status(ch)})
        elif op == "append":
            if MODE == "hang_on_append":
                continue  # never respond
            ch = params["channel"]
            if not require_open(rid, ch):
                continue
            if MODE != "commit_never_lands":
                committed[ch] = params["end_token"]
            errors[ch] = rejected_count(ch) + sum(1 for row in rows if "REJECT" in row)
            persist()
            reply({"id": rid, "ok": True, "result": {"appended": len(rows)}})
        elif op == "wait_commit":
            ch = params["channel"]
            if not require_open(rid, ch):
                continue
            tok = committed.get(ch)
            if tok == params["token"]:
                reply({"id": rid, "ok": True, "result": status(ch)})
            else:
                reply({"id": rid, "ok": False, "error": f"token {params['token']} not committed (at {tok})", "code": "timeout"})
        elif op == "channel_status":
            ch = params["channel"]
            if not require_open(rid, ch):
                continue
            reply({"id": rid, "ok": True, "result": status(ch)})
        elif op == "close_channel":
            ch = params["channel"]
            if not require_open(rid, ch):
                continue
            # The handle goes either way, but only a drop reaches "Snowflake",
            # where it discards everything the channel had accumulated — which is
            # what a reopen of the same name then finds, as verified against
            # Snowflake itself by TestStreamV2Manager.
            opened.discard(ch)
            if params.get("drop"):
                committed.pop(ch, None)
                errors[ch] = 0
                persist()
            reply({"id": rid, "ok": True})
        elif op == "shutdown":
            log("info", "fake sidecar shutting down")
            reply({"id": rid, "ok": True})
            sys.exit(0)
        else:
            reply({"id": rid, "ok": False, "error": f"unknown op {op}", "code": "protocol"})

    if MODE == "ignore_sigterm":
        # Outlive the connection so only SIGKILL can end the process.
        while True:
            signal.pause()


if __name__ == "__main__":
    main()
