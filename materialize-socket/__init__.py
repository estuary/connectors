"""
Estuary Flow Source Connector for Generic Socket Connections.

This connector captures data from TCP/TLS socket connections with support for
multiple codecs (plain, line-delimited, JSON).
"""

import asyncio
import json
import ssl
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import AsyncIterator, Optional, Any, Dict, List, Callable, Awaitable
from abc import ABC, abstractmethod
from pydantic import BaseModel, Field, SecretStr, field_validator


# =============================================================================
# Configuration Models
# =============================================================================

class CodecType(str, Enum):
    """Supported codec types for parsing incoming data."""
    PLAIN = "plain"  # Raw bytes as string
    LINE = "line"  # Line-delimited text (newline separated)
    JSON = "json"  # JSON objects (one per line or streaming)


class TLSConfig(BaseModel):
    """TLS/SSL configuration for secure connections."""
    enabled: bool = Field(
        default=False,
        description="Enable TLS/SSL encryption for the connection"
    )
    verify_cert: bool = Field(
        default=True,
        description="Verify server certificate"
    )
    ca_cert: Optional[str] = Field(
        default=None,
        description="Path to CA certificate file or PEM-encoded certificate string"
    )
    client_cert: Optional[str] = Field(
        default=None,
        description="Path to client certificate file for mutual TLS"
    )
    client_key: Optional[SecretStr] = Field(
        default=None,
        description="Path to client private key file for mutual TLS"
    )
    server_hostname: Optional[str] = Field(
        default=None,
        description="Expected server hostname for certificate verification"
    )


class SocketEndpointConfig(BaseModel):
    """Configuration for a socket endpoint connection."""
    host: str = Field(
        ...,
        description="Hostname or IP address to connect to"
    )
    port: int = Field(
        ...,
        ge=1,
        le=65535,
        description="Port number to connect to"
    )
    tls: TLSConfig = Field(
        default_factory=TLSConfig,
        description="TLS/SSL configuration"
    )
    codec: CodecType = Field(
        default=CodecType.LINE,
        description="Codec for parsing incoming data: plain, line, or json"
    )
    buffer_size: int = Field(
        default=65536,
        ge=1024,
        le=16777216,
        description="Read buffer size in bytes"
    )
    reconnect_delay: float = Field(
        default=5.0,
        ge=0.1,
        le=300.0,
        description="Delay in seconds before reconnecting after connection loss"
    )
    max_reconnect_attempts: int = Field(
        default=-1,
        ge=-1,
        description="Maximum reconnection attempts (-1 for unlimited)"
    )
    connection_timeout: float = Field(
        default=30.0,
        ge=1.0,
        le=300.0,
        description="Connection timeout in seconds"
    )
    idle_timeout: Optional[float] = Field(
        default=None,
        ge=1.0,
        description="Close connection after this many seconds of no data (None for no timeout)"
    )

    @field_validator('host')
    @classmethod
    def validate_host(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Host cannot be empty")
        return v.strip()


class SocketResourceConfig(BaseModel):
    """Resource configuration for socket capture binding."""
    stream: str = Field(
        ...,
        description="Name of the stream/collection to capture into"
    )
    namespace: Optional[str] = Field(
        default=None,
        description="Optional namespace for the stream"
    )


# =============================================================================
# Document Models
# =============================================================================

class SocketDocument(BaseModel):
    """A document captured from a socket connection."""
    _meta: Dict[str, Any] = Field(
        default_factory=dict,
        description="Metadata about the document"
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp when the document was received"
    )
    source_host: str = Field(
        ...,
        description="Source host from which data was received"
    )
    source_port: int = Field(
        ...,
        description="Source port from which data was received"
    )
    data: Any = Field(
        ...,
        description="The actual data received"
    )
    raw: Optional[str] = Field(
        default=None,
        description="Raw string representation (for non-JSON codecs)"
    )
    sequence: int = Field(
        default=0,
        description="Sequence number within the session"
    )
    session_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique session identifier"
    )


# =============================================================================
# Codec Implementations
# =============================================================================

class BaseCodec(ABC):
    """Base class for data codecs."""

    @abstractmethod
    async def decode(self, data: bytes) -> AsyncIterator[Any]:
        """Decode bytes into data objects."""
        pass

    @abstractmethod
    def reset(self) -> None:
        """Reset codec state (e.g., clear buffers)."""
        pass


class PlainCodec(BaseCodec):
    """Plain text codec - emits raw string data."""

    async def decode(self, data: bytes) -> AsyncIterator[str]:
        if data:
            yield data.decode('utf-8', errors='replace')

    def reset(self) -> None:
        pass


class LineCodec(BaseCodec):
    """Line-delimited codec - splits data on newlines."""

    def __init__(self):
        self._buffer = ""

    async def decode(self, data: bytes) -> AsyncIterator[str]:
        self._buffer += data.decode('utf-8', errors='replace')

        while '\n' in self._buffer:
            line, self._buffer = self._buffer.split('\n', 1)
            line = line.rstrip('\r')  # Handle CRLF
            if line:  # Skip empty lines
                yield line

    def reset(self) -> None:
        self._buffer = ""


class JSONCodec(BaseCodec):
    """JSON codec - parses JSON objects from newline-delimited JSON."""

    def __init__(self):
        self._line_codec = LineCodec()

    async def decode(self, data: bytes) -> AsyncIterator[Dict[str, Any]]:
        async for line in self._line_codec.decode(data):
            try:
                yield json.loads(line)
            except json.JSONDecodeError as e:
                # Emit error document for malformed JSON
                yield {
                    "_error": True,
                    "_error_message": str(e),
                    "_raw": line
                }

    def reset(self) -> None:
        self._line_codec.reset()


def get_codec(codec_type: CodecType) -> BaseCodec:
    """Factory function to get codec by type."""
    codecs = {
        CodecType.PLAIN: PlainCodec,
        CodecType.LINE: LineCodec,
        CodecType.JSON: JSONCodec,
    }
    return codecs[codec_type]()


# =============================================================================
# Socket Connection Handler
# =============================================================================

class SocketConnection:
    """Handles socket connection and data streaming."""

    def __init__(self, config: SocketEndpointConfig):
        self.config = config
        self.codec = get_codec(config.codec)
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._connected = False
        self._session_id = str(uuid.uuid4())
        self._sequence = 0

    @property
    def connected(self) -> bool:
        return self._connected

    async def _create_ssl_context(self) -> Optional[ssl.SSLContext]:
        """Create SSL context if TLS is enabled."""
        if not self.config.tls.enabled:
            return None

        context = ssl.create_default_context()

        if not self.config.tls.verify_cert:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE

        if self.config.tls.ca_cert:
            if self.config.tls.ca_cert.startswith('-----BEGIN'):
                # PEM-encoded certificate string
                context.load_verify_locations(cadata=self.config.tls.ca_cert)
            else:
                # File path
                context.load_verify_locations(cafile=self.config.tls.ca_cert)

        if self.config.tls.client_cert and self.config.tls.client_key:
            key_path = self.config.tls.client_key.get_secret_value() if self.config.tls.client_key else None
            context.load_cert_chain(
                certfile=self.config.tls.client_cert,
                keyfile=key_path
            )

        return context

    async def connect(self) -> None:
        """Establish connection to the socket endpoint."""
        ssl_context = await self._create_ssl_context()
        server_hostname = self.config.tls.server_hostname or self.config.host

        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(
                self.config.host,
                self.config.port,
                ssl=ssl_context,
                server_hostname=server_hostname if ssl_context else None
            ),
            timeout=self.config.connection_timeout
        )

        self._connected = True
        self._session_id = str(uuid.uuid4())
        self._sequence = 0
        self.codec.reset()

    async def disconnect(self) -> None:
        """Close the connection."""
        self._connected = False
        if self._writer:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass
        self._reader = None
        self._writer = None

    async def read_documents(self) -> AsyncIterator[SocketDocument]:
        """Read and yield documents from the socket."""
        if not self._reader:
            raise RuntimeError("Not connected")

        while self._connected:
            try:
                if self.config.idle_timeout:
                    data = await asyncio.wait_for(
                        self._reader.read(self.config.buffer_size),
                        timeout=self.config.idle_timeout
                    )
                else:
                    data = await self._reader.read(self.config.buffer_size)

                if not data:
                    # Connection closed by remote
                    break

                async for decoded in self.codec.decode(data):
                    self._sequence += 1

                    doc = SocketDocument(
                        timestamp=datetime.now(timezone.utc),
                        source_host=self.config.host,
                        source_port=self.config.port,
                        data=decoded if isinstance(decoded, dict) else {"value": decoded},
                        raw=decoded if isinstance(decoded, str) else None,
                        sequence=self._sequence,
                        session_id=self._session_id
                    )
                    yield doc

            except asyncio.TimeoutError:
                # Idle timeout reached
                break
            except (ConnectionError, BrokenPipeError, OSError):
                break

        await self.disconnect()


# =============================================================================
# Capture State Management
# =============================================================================

class CaptureState(BaseModel):
    """Checkpoint state for the capture."""
    last_sequence: int = 0
    last_session_id: Optional[str] = None
    last_timestamp: Optional[datetime] = None
    total_documents: int = 0
    total_bytes: int = 0
    reconnect_count: int = 0


# =============================================================================
# Main Capture Logic
# =============================================================================

class SocketCapture:
    """Main capture class for socket connector."""

    def __init__(self, config: SocketEndpointConfig):
        self.config = config
        self.connection = SocketConnection(config)
        self.state = CaptureState()
        self._running = False

    async def start(
            self,
            on_document: Callable[[SocketDocument], Awaitable[None]],
            on_checkpoint: Optional[Callable[[CaptureState], Awaitable[None]]] = None
    ) -> None:
        """
        Start capturing data from the socket.

        Args:
            on_document: Async callback for each captured document
            on_checkpoint: Optional async callback for checkpoints
        """
        self._running = True
        reconnect_count = 0

        while self._running:
            try:
                await self.connection.connect()
                reconnect_count = 0
                self.state.reconnect_count += 1

                async for doc in self.connection.read_documents():
                    if not self._running:
                        break

                    self.state.last_sequence = doc.sequence
                    self.state.last_session_id = doc.session_id
                    self.state.last_timestamp = doc.timestamp
                    self.state.total_documents += 1

                    await on_document(doc)

                    # Periodic checkpoint
                    if self.state.total_documents % 1000 == 0 and on_checkpoint:
                        await on_checkpoint(self.state)

            except asyncio.TimeoutError:
                pass  # Connection timeout, will retry
            except Exception as e:
                if not self._running:
                    break

                reconnect_count += 1
                max_attempts = self.config.max_reconnect_attempts

                if max_attempts >= 0 and reconnect_count > max_attempts:
                    raise RuntimeError(
                        f"Max reconnection attempts ({max_attempts}) exceeded"
                    ) from e

                await asyncio.sleep(self.config.reconnect_delay)

            finally:
                await self.connection.disconnect()

            if not self._running:
                break

            # Wait before reconnecting
            await asyncio.sleep(self.config.reconnect_delay)

    async def stop(self) -> None:
        """Stop the capture."""
        self._running = False
        await self.connection.disconnect()


# =============================================================================
# Connector Entry Points (Flow Protocol)
# =============================================================================

def spec() -> Dict[str, Any]:
    """Return the connector specification."""
    return {
        "configSchema": SocketEndpointConfig.model_json_schema(),
        "resourceConfigSchema": SocketResourceConfig.model_json_schema(),
        "documentationUrl": "https://docs.estuary.dev/reference/connectors/source-socket/",
    }


async def discover(config: Dict[str, Any]) -> Dict[str, Any]:
    """Discover available streams/bindings."""
    endpoint_config = SocketEndpointConfig(**config)

    # For socket connector, we typically have a single stream
    return {
        "bindings": [
            {
                "recommendedName": f"socket_{endpoint_config.host}_{endpoint_config.port}",
                "resourceConfig": {
                    "stream": f"socket_{endpoint_config.host}_{endpoint_config.port}"
                },
                "documentSchema": SocketDocument.model_json_schema(),
                "key": ["/_meta/timestamp", "/sequence", "/session_id"],
            }
        ]
    }


async def validate(config: Dict[str, Any]) -> Dict[str, Any]:
    """Validate the configuration by attempting a connection."""
    endpoint_config = SocketEndpointConfig(**config)
    connection = SocketConnection(endpoint_config)

    try:
        await connection.connect()
        await connection.disconnect()
        return {"valid": True}
    except Exception as e:
        return {
            "valid": False,
            "error": str(e)
        }


async def capture(
        config: Dict[str, Any],
        bindings: List[Dict[str, Any]],
        state: Optional[Dict[str, Any]] = None
) -> AsyncIterator[Dict[str, Any]]:
    """
    Run the capture, yielding documents.

    This is a simplified capture generator that yields Flow-compatible
    checkpoint and document messages.
    """
    endpoint_config = SocketEndpointConfig(**config)
    capture_state = CaptureState(**(state or {}))

    socket_capture = SocketCapture(endpoint_config)
    document_queue: asyncio.Queue = asyncio.Queue()

    async def on_document(doc: SocketDocument):
        await document_queue.put(("document", doc.model_dump()))

    async def on_checkpoint(state: CaptureState):
        await document_queue.put(("checkpoint", state.model_dump()))

    # Start capture in background
    capture_task = asyncio.create_task(
        socket_capture.start(on_document, on_checkpoint)
    )

    try:
        while True:
            try:
                msg_type, data = await asyncio.wait_for(
                    document_queue.get(),
                    timeout=1.0
                )

                if msg_type == "document":
                    yield {
                        "type": "document",
                        "binding": 0,
                        "document": data
                    }
                elif msg_type == "checkpoint":
                    yield {
                        "type": "checkpoint",
                        "state": data
                    }

            except asyncio.TimeoutError:
                # Check if capture task is still running
                if capture_task.done():
                    exc = capture_task.exception()
                    if exc:
                        raise exc
                    break

    finally:
        await socket_capture.stop()
        if not capture_task.done():
            capture_task.cancel()
            try:
                await capture_task
            except asyncio.CancelledError:
                pass


# =============================================================================
# CLI Entry Point
# =============================================================================

def main():
    """CLI entry point for the connector."""
    import sys

    if len(sys.argv) < 2:
        print("Usage: source-socket <command> [args]")
        print("Commands: spec, discover, validate, capture")
        sys.exit(1)

    command = sys.argv[1]

    if command == "spec":
        print(json.dumps(spec(), indent=2))
    elif command == "discover":
        config = json.loads(sys.stdin.read())
        result = asyncio.run(discover(config))
        print(json.dumps(result, indent=2))
    elif command == "validate":
        config = json.loads(sys.stdin.read())
        result = asyncio.run(validate(config))
        print(json.dumps(result, indent=2))
    elif command == "capture":
        input_data = json.loads(sys.stdin.read())
        config = input_data.get("config", {})
        bindings = input_data.get("bindings", [])
        state = input_data.get("state")

        async def run_capture():
            async for msg in capture(config, bindings, state):
                print(json.dumps(msg, default=str))
                sys.stdout.flush()

        asyncio.run(run_capture())
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()