"""Owns snowpipe-streaming SDK objects behind the flat RPC channel namespace.

The high-performance SDK scopes a client to a single pipe (one auto-created
default pipe per table), so a client is created per (database, schema, table)
and channels are tracked by their RPC channel name.
"""

import logging
import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger("snowpipe_sidecar.channels")

# RECEIVER_SATURATED is the SDK's flow-control signal: its internal buffer is
# full and the caller should back off, not fail. The total wait stays under the
# Go client's append RPC timeout so a genuinely wedged channel still surfaces
# as an error there.
_SATURATION_MAX_WAIT_S = 90.0
_SATURATION_INITIAL_DELAY_S = 0.5
_SATURATION_MAX_DELAY_S = 10.0


class SidecarError(Exception):
    """An error with a stable code, surfaced through an RPC error response."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _default_client_factory(props: Dict[str, Any], database: str, schema: str, table: str):
    # Imported here so that process environment (SS_LOG_TARGET et al) is
    # settled before the SDK's native core initializes.
    from snowflake.ingest.streaming import StreamingIngestClient

    return StreamingIngestClient.from_table(
        client_name=f"estuary_flow_{uuid.uuid4().hex[:12]}",
        db_name=database,
        schema_name=schema,
        table_name=table,
        properties=props,
    )


def _wrap_sdk_error(err: Exception) -> SidecarError:
    code = "sdk_error"
    error_code = getattr(err, "error_code", None)
    if error_code is not None:
        code = str(getattr(error_code, "name", error_code))
    return SidecarError(code, str(err))


class ChannelManager:
    def __init__(self, props: Dict[str, Any], client_factory: Optional[Callable] = None):
        self._props = props
        self._client_factory = client_factory or _default_client_factory
        self._mu = threading.Lock()
        self._clients: Dict[Tuple[str, str, str], Any] = {}
        self._channels: Dict[str, Any] = {}

    def open(self, database: str, schema: str, table: str, channel: str) -> Optional[str]:
        """Open (or reopen) a channel, returning Snowflake's authoritative
        latest committed offset token for it."""
        key = (database, schema, table)
        try:
            with self._mu:
                client = self._clients.get(key)
                if client is None:
                    client = self._client_factory(self._props, database, schema, table)
                    self._clients[key] = client
            ch, status = client.open_channel(channel)
            with self._mu:
                self._channels[channel] = ch
        except Exception as err:
            raise _wrap_sdk_error(err) from err

        token = status.latest_committed_offset_token
        logger.info(
            "opened channel",
            extra={"channel": channel, "table": table, "committedToken": token},
        )
        return token

    def _channel(self, channel: str):
        with self._mu:
            ch = self._channels.get(channel)
        if ch is None:
            raise SidecarError("unknown_channel", f"channel {channel!r} is not open")
        return ch

    def append(self, channel: str, token: str, rows: List[Dict[str, Any]]) -> int:
        ch = self._channel(channel)
        delay = _SATURATION_INITIAL_DELAY_S
        deadline = time.monotonic() + _SATURATION_MAX_WAIT_S
        while True:
            try:
                ch.append_rows(rows, token, token)
            except (ValueError, TypeError) as err:
                raise SidecarError("invalid_rows", str(err)) from err
            except Exception as err:
                code = getattr(err, "error_code", None)
                name = str(getattr(code, "name", code or ""))
                if "RECEIVER_SATURATED" in name and time.monotonic() + delay < deadline:
                    logger.warning(
                        "streaming receiver saturated; backing off",
                        extra={"channel": channel, "delaySeconds": delay},
                    )
                    time.sleep(delay)
                    delay = min(delay * 2, _SATURATION_MAX_DELAY_S)
                    continue
                raise _wrap_sdk_error(err) from err
            return len(rows)

    def wait_commit(self, channel: str, token: str, timeout_s: float) -> Optional[str]:
        ch = self._channel(channel)
        try:
            ch.wait_for_commit(lambda t: t == token, timeout_seconds=int(timeout_s))
        except Exception as err:
            raise _wrap_sdk_error(err) from err
        return ch.get_latest_committed_offset_token()

    def status(self, channel: str) -> Dict[str, Any]:
        ch = self._channel(channel)
        try:
            st = ch.get_channel_status()
        except Exception as err:
            raise _wrap_sdk_error(err) from err
        return {
            "committed_token": st.latest_committed_offset_token,
            "rows_error_count": st.rows_error_count,
            "last_error_message": st.last_error_message,
        }

    def close_channel(self, channel: str) -> None:
        with self._mu:
            ch = self._channels.pop(channel, None)
        if ch is None:
            return
        try:
            ch.close()
        except Exception as err:
            raise _wrap_sdk_error(err) from err

    def close(self) -> None:
        """Close all channels then clients, flushing buffered data."""
        with self._mu:
            channels = list(self._channels.values())
            clients = list(self._clients.values())
            self._channels.clear()
            self._clients.clear()
        for ch in channels:
            try:
                ch.close()
            except Exception:
                logger.warning("closing channel during shutdown", exc_info=True)
        for client in clients:
            try:
                client.close()
            except Exception:
                logger.warning("closing client during shutdown", exc_info=True)
