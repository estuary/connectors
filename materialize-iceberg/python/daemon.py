"""Long-lived PySpark daemon for materialize-iceberg integration tests.

Runs inside the spark-daemon container, holds a single SparkSession that is
preconfigured with the Iceberg catalog, OAuth credentials, and S3-compatible
endpoint. Accepts JSON job submissions over HTTP and dispatches them to the
same `run(spark, input)` functions that EMR scripts call. Each job runs
inside the existing JVM driver, so submission overhead drops from ~13s
(spark-submit cold start) to a few hundred milliseconds.
"""

from __future__ import annotations

import json
import os
import sys
import threading
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from common import get_spark_session_from_env

import exec as exec_module
import load as load_module
import merge as merge_module

_ready = threading.Event()
_session = None
_init_error: str | None = None
# Spark's driver is single-threaded for SQL submissions; serializing job
# requests keeps the daemon's behavior identical to back-to-back spark-submit
# invocations.
_run_lock = threading.Lock()

ACTIONS = {
    "load": load_module.run,
    "merge": merge_module.run,
    "exec": exec_module.run,
}


def init_session():
    global _session, _init_error
    try:
        _session = get_spark_session_from_env(shared_across_submissions=True)
        _ready.set()
        print("spark daemon: session ready", file=sys.stderr, flush=True)
    except Exception as e:
        _init_error = f"{e}\n{traceback.format_exc()}"
        print(f"spark daemon: init failed: {_init_error}", file=sys.stderr, flush=True)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            if _ready.is_set():
                self._respond(200, {"ok": True})
            elif _init_error:
                self._respond(500, {"ok": False, "error": _init_error})
            else:
                self._respond(503, {"ok": False, "starting": True})
        else:
            self._respond(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/run":
            self._respond(404, {"error": "not found"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            req = json.loads(body)
            action = req["action"]
            input_data = req["input"]
        except Exception as e:
            self._respond(400, {"success": False, "error": f"bad request: {e}"})
            return

        fn = ACTIONS.get(action)
        if fn is None:
            self._respond(400, {"success": False, "error": f"unknown action: {action}"})
            return

        if not _ready.is_set():
            self._respond(
                503,
                {"success": False, "error": _init_error or "session not ready"},
            )
            return

        try:
            with _run_lock:
                fn(_session, input_data)
            self._respond(200, {"success": True})
        except Exception as e:
            err = f"{e}\n{traceback.format_exc()}"
            print(f"spark daemon: job error: {err}", file=sys.stderr, flush=True)
            self._respond(200, {"success": False, "error": err})

    def _respond(self, status: int, obj):
        body = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        # The daemon already logs errors to stderr; suppress per-request
        # access logs to keep the test output clean.
        return


if __name__ == "__main__":
    threading.Thread(target=init_session, daemon=True).start()
    port = int(os.environ.get("SPARK_DAEMON_PORT", "9806"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"spark daemon: listening on :{port}", file=sys.stderr, flush=True)
    server.serve_forever()
