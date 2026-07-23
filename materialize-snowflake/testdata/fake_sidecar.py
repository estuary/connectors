"""A stdlib-only fake of the snowpipe streaming sidecar, for Go unit tests.

Speaks the same transport handshake and NDJSON RPC protocol as the real
sidecar, backed by an in-memory "Snowflake" that commits appends instantly.

Fault injection via the FAKE_SIDECAR_MODE environment variable:
  never_ready           never prints the ready line
  exit_before_ready     exits 7 before printing the ready line
  crash_after_configure prints garbage to stderr and exits 9 after configure
  hang_on_append        accepts appends but never responds to them
  garbage_stderr        emits non-JSON stderr noise alongside normal operation
  ignore_sigterm        ignores SIGTERM so only SIGKILL can end it
"""

import json
import os
import signal
import socket
import sys
import threading

MODE = os.environ.get("FAKE_SIDECAR_MODE", "")


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

    if "--uds" in argv:
        path = argv[argv.index("--uds") + 1]
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(path)
        ready = {"ready": True, "transport": "uds"}
    else:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(("127.0.0.1", 0))
        ready = {"ready": True, "transport": "tcp", "port": server.getsockname()[1]}

    server.listen(1)
    sys.stdout.write(json.dumps(ready) + "\n")
    sys.stdout.flush()

    conn, _ = server.accept()
    rfile = conn.makefile("r", encoding="utf-8")
    wlock = threading.Lock()

    if MODE == "garbage_stderr":
        sys.stderr.write("2026-07-17T00:00:00Z | WARN | core (1) | native noise, not JSON\n")
        sys.stderr.write('{"level":"warning","msg":"structured sidecar warning","fields":{"source":"fake"}}\n')
        sys.stderr.flush()

    committed = {}  # channel -> token
    configured = False

    def reply(res):
        with wlock:
            conn.sendall((json.dumps(res) + "\n").encode())

    for line in rfile:
        req = json.loads(line)
        rid, op = req["id"], req["op"]
        params = req.get("params") or {}

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
            reply({"id": rid, "ok": True, "result": {"committed_token": committed.get(ch)}})
        elif op == "append":
            if MODE == "hang_on_append":
                continue  # never respond
            committed[params["channel"]] = params["token"]
            reply({"id": rid, "ok": True, "result": {"appended": len(params["rows"])}})
        elif op == "wait_commit":
            tok = committed.get(params["channel"])
            if tok == params["token"]:
                reply({"id": rid, "ok": True, "result": {"committed_token": tok}})
            else:
                reply({"id": rid, "ok": False, "error": f"token {params['token']} not committed (at {tok})", "code": "timeout"})
        elif op == "channel_status":
            reply({"id": rid, "ok": True, "result": {
                "committed_token": committed.get(params["channel"]),
                "rows_error_count": 0,
                "last_error_message": "",
            }})
        elif op == "close_channel":
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
