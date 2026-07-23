"""NDJSON RPC server over a single accepted connection.

Threading model: the reader loop parses requests and routes them to workers.
Control ops (configure, open_channel, shutdown) run serially on one control
worker. Channel ops run on a per-channel worker, so a long wait_commit on one
channel never blocks appends to another, while ops on the same channel stay
strictly ordered. Responses are serialized by a write lock.

Failure policy is crash-only: any unhandled exception logs a traceback and
exits non-zero, letting the Go supervisor surface the failure.
"""

import json
import logging
import os
import queue
import threading
from typing import Any, Callable, Dict, Optional

from .channels import ChannelManager, SidecarError

logger = logging.getLogger("snowpipe_sidecar.server")

_CHANNEL_OPS = frozenset(["append", "wait_commit", "channel_status", "close_channel"])


class Server:
    def __init__(
        self,
        conn,
        auth_token: str,
        manager_factory: Callable[[Dict[str, Any]], ChannelManager] = ChannelManager,
        exit_fn: Callable[[int], None] = os._exit,
    ):
        self._conn = conn
        self._auth_token = auth_token
        self._manager_factory = manager_factory
        self._exit = exit_fn

        self._wlock = threading.Lock()
        self._manager: Optional[ChannelManager] = None

        self._control: "queue.Queue" = queue.Queue()
        self._channel_queues: Dict[str, "queue.Queue"] = {}

    def serve(self) -> None:
        threading.Thread(target=self._worker, args=(self._control,), daemon=True).start()

        rfile = self._conn.makefile("r", encoding="utf-8")
        for line in rfile:
            if not line.strip():
                continue
            try:
                req = json.loads(line)
                rid, op = req["id"], req["op"]
            except (ValueError, KeyError) as err:
                logger.error("malformed request", extra={"error": str(err)})
                self._exit(3)
                return
            params = req.get("params") or {}

            if op in _CHANNEL_OPS:
                q = self._channel_queues.get(params.get("channel"))
                if q is None:
                    self._reply_error(rid, SidecarError("unknown_channel", f"channel {params.get('channel')!r} is not open"))
                    continue
                q.put((rid, op, params))
            else:
                self._control.put((rid, op, params))

        # The Go side hanging up outside of a graceful shutdown means the
        # connector is gone; there is nobody left to serve.
        logger.info("connection closed; exiting")
        self._exit(0)

    def _worker(self, q: "queue.Queue") -> None:
        while True:
            rid, op, params = q.get()
            try:
                self._dispatch(rid, op, params)
            except SidecarError as err:
                self._reply_error(rid, err)
            except Exception:
                logger.error("unhandled error in sidecar worker", exc_info=True)
                self._exit(3)
                return

    def _dispatch(self, rid: int, op: str, params: Dict[str, Any]) -> None:
        if op == "configure":
            if params.get("auth") != self._auth_token:
                raise SidecarError("auth", "bad auth token")
            self._manager = self._manager_factory(params["profile"])
            logger.info("sidecar configured")
            self._reply(rid, None)
            return

        if self._manager is None:
            raise SidecarError("protocol", "configure must be the first request")

        if op == "open_channel":
            token = self._manager.open(
                params["database"], params["schema"], params["table"], params["channel"]
            )
            # The channel worker must exist before the caller sees success.
            if params["channel"] not in self._channel_queues:
                q: "queue.Queue" = queue.Queue()
                self._channel_queues[params["channel"]] = q
                threading.Thread(target=self._worker, args=(q,), daemon=True).start()
            self._reply(rid, {"committed_token": token})
        elif op == "append":
            n = self._manager.append(params["channel"], params["token"], params["rows"])
            self._reply(rid, {"appended": n})
        elif op == "wait_commit":
            token = self._manager.wait_commit(
                params["channel"], params["token"], params["timeout_s"]
            )
            self._reply(rid, {"committed_token": token})
        elif op == "channel_status":
            self._reply(rid, self._manager.status(params["channel"]))
        elif op == "close_channel":
            self._manager.close_channel(params["channel"])
            self._reply(rid, None)
        elif op == "shutdown":
            logger.info("shutting down on request")
            self._manager.close()
            self._reply(rid, None)
            self._exit(0)
        else:
            raise SidecarError("protocol", f"unknown op {op!r}")

    def _reply(self, rid: int, result: Optional[Dict[str, Any]]) -> None:
        res: Dict[str, Any] = {"id": rid, "ok": True}
        if result is not None:
            res["result"] = result
        self._send(res)

    def _reply_error(self, rid: int, err: SidecarError) -> None:
        self._send({"id": rid, "ok": False, "error": str(err), "code": err.code})

    def _send(self, res: Dict[str, Any]) -> None:
        data = (json.dumps(res) + "\n").encode()
        with self._wlock:
            self._conn.sendall(data)
