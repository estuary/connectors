"""
Estuary Flow Source Connector for AMQP (Advanced Message Queuing Protocol).

This connector captures messages from AMQP-compatible message brokers like
RabbitMQ, with support for routing keys, priorities, and various authentication methods.
"""

import asyncio
import json
import ssl
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import AsyncIterator, Optional, Any, Dict, List, Callable, Awaitable, Union
from abc import ABC, abstractmethod
from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator


# Note: In production, you would use aio-pika or similar async AMQP library
# For this implementation, we define the interfaces and mock the client for structure


# =============================================================================
# Configuration Models
# =============================================================================

class AuthType(str, Enum):
    """Authentication types for AMQP connection."""
    PLAIN = "plain"  # Username/password
    EXTERNAL = "external"  # External authentication (e.g., certificates)
    AMQPLAIN = "amqplain"  # AMQP PLAIN mechanism


class ExchangeType(str, Enum):
    """AMQP exchange types."""
    DIRECT = "direct"
    FANOUT = "fanout"
    TOPIC = "topic"
    HEADERS = "headers"


class TLSConfig(BaseModel):
    """TLS/SSL configuration for secure AMQP connections."""
    enabled: bool = Field(
        default=False,
        description="Enable TLS/SSL encryption (amqps://)"
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


class QueueConfig(BaseModel):
    """Configuration for AMQP queue."""
    name: str = Field(
        ...,
        description="Queue name to consume from"
    )
    durable: bool = Field(
        default=True,
        description="Queue survives broker restart"
    )
    exclusive: bool = Field(
        default=False,
        description="Queue is exclusive to this connection"
    )
    auto_delete: bool = Field(
        default=False,
        description="Queue is deleted when last consumer disconnects"
    )
    arguments: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Additional queue arguments (x-arguments)"
    )


class ExchangeBindingConfig(BaseModel):
    """Configuration for binding queue to exchange."""
    exchange: str = Field(
        ...,
        description="Exchange name to bind to"
    )
    routing_key: str = Field(
        default="#",
        description="Routing key pattern for binding"
    )
    arguments: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Additional binding arguments"
    )


class ConsumerConfig(BaseModel):
    """Consumer-specific configuration."""
    prefetch_count: int = Field(
        default=100,
        ge=1,
        le=65535,
        description="Number of unacknowledged messages to prefetch"
    )
    consumer_tag: Optional[str] = Field(
        default=None,
        description="Consumer tag (auto-generated if not specified)"
    )
    no_ack: bool = Field(
        default=False,
        description="Disable message acknowledgments (not recommended for production)"
    )
    exclusive: bool = Field(
        default=False,
        description="Request exclusive consumer access"
    )
    priority: Optional[int] = Field(
        default=None,
        ge=0,
        le=255,
        description="Consumer priority (higher = more messages)"
    )


class AMQPEndpointConfig(BaseModel):
    """Configuration for AMQP endpoint connection."""

    # Connection settings
    host: str = Field(
        default="localhost",
        description="AMQP broker hostname"
    )
    port: int = Field(
        default=5672,
        ge=1,
        le=65535,
        description="AMQP broker port (5672 for plain, 5671 for TLS)"
    )
    virtual_host: str = Field(
        default="/",
        description="AMQP virtual host"
    )

    # Authentication
    auth_type: AuthType = Field(
        default=AuthType.PLAIN,
        description="Authentication mechanism"
    )
    username: str = Field(
        default="guest",
        description="Username for authentication"
    )
    password: SecretStr = Field(
        default=SecretStr("guest"),
        description="Password for authentication"
    )

    # TLS
    tls: TLSConfig = Field(
        default_factory=TLSConfig,
        description="TLS/SSL configuration"
    )

    # Queue and binding
    queue: QueueConfig = Field(
        ...,
        description="Queue configuration"
    )
    bindings: List[ExchangeBindingConfig] = Field(
        default_factory=list,
        description="Exchange bindings for the queue"
    )

    # Consumer
    consumer: ConsumerConfig = Field(
        default_factory=ConsumerConfig,
        description="Consumer configuration"
    )

    # Connection management
    heartbeat: int = Field(
        default=60,
        ge=0,
        le=3600,
        description="Heartbeat interval in seconds (0 to disable)"
    )
    connection_timeout: float = Field(
        default=30.0,
        ge=1.0,
        le=300.0,
        description="Connection timeout in seconds"
    )
    reconnect_delay: float = Field(
        default=5.0,
        ge=0.1,
        le=300.0,
        description="Delay before reconnecting after connection loss"
    )
    max_reconnect_attempts: int = Field(
        default=-1,
        ge=-1,
        description="Maximum reconnection attempts (-1 for unlimited)"
    )

    # Message handling
    message_ttl: Optional[int] = Field(
        default=None,
        ge=0,
        description="Message time-to-live in milliseconds"
    )
    max_priority: Optional[int] = Field(
        default=None,
        ge=0,
        le=255,
        description="Maximum message priority (enables priority queue)"
    )

    @field_validator('host')
    @classmethod
    def validate_host(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Host cannot be empty")
        return v.strip()

    @model_validator(mode='after')
    def validate_tls_port(self):
        """Adjust default port based on TLS setting."""
        if self.tls.enabled and self.port == 5672:
            # Suggest TLS port but don't force it
            pass
        return self

    def get_amqp_url(self) -> str:
        """Generate AMQP connection URL."""
        scheme = "amqps" if self.tls.enabled else "amqp"
        password = self.password.get_secret_value()
        return f"{scheme}://{self.username}:{password}@{self.host}:{self.port}/{self.virtual_host}"


class AMQPResourceConfig(BaseModel):
    """Resource configuration for AMQP capture binding."""
    stream: str = Field(
        ...,
        description="Name of the stream/collection to capture into"
    )
    namespace: Optional[str] = Field(
        default=None,
        description="Optional namespace for the stream"
    )
    routing_key_filter: Optional[str] = Field(
        default=None,
        description="Additional routing key filter for this binding"
    )


# =============================================================================
# Message Models
# =============================================================================

class AMQPMessageProperties(BaseModel):
    """AMQP message properties."""
    content_type: Optional[str] = Field(default=None, description="MIME content type")
    content_encoding: Optional[str] = Field(default=None, description="Content encoding")
    headers: Optional[Dict[str, Any]] = Field(default=None, description="Message headers")
    delivery_mode: Optional[int] = Field(default=None, description="Delivery mode (1=transient, 2=persistent)")
    priority: Optional[int] = Field(default=None, ge=0, le=255, description="Message priority")
    correlation_id: Optional[str] = Field(default=None, description="Correlation ID")
    reply_to: Optional[str] = Field(default=None, description="Reply-to queue/routing key")
    expiration: Optional[str] = Field(default=None, description="Message expiration")
    message_id: Optional[str] = Field(default=None, description="Message ID")
    timestamp: Optional[datetime] = Field(default=None, description="Message timestamp")
    type: Optional[str] = Field(default=None, description="Message type")
    user_id: Optional[str] = Field(default=None, description="User ID")
    app_id: Optional[str] = Field(default=None, description="Application ID")


class AMQPDeliveryInfo(BaseModel):
    """AMQP delivery information."""
    consumer_tag: str = Field(..., description="Consumer tag")
    delivery_tag: int = Field(..., description="Delivery tag for acknowledgment")
    redelivered: bool = Field(default=False, description="Message was redelivered")
    exchange: str = Field(default="", description="Source exchange")
    routing_key: str = Field(default="", description="Routing key used")


class AMQPDocument(BaseModel):
    """A document captured from an AMQP message."""
    _meta: Dict[str, Any] = Field(
        default_factory=dict,
        description="Metadata about the document"
    )

    # Timing
    received_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp when message was received by connector"
    )

    # Message content
    body: Any = Field(
        ...,
        description="Message body (parsed if JSON, otherwise string)"
    )
    body_raw: Optional[str] = Field(
        default=None,
        description="Raw message body as string"
    )
    body_size: int = Field(
        default=0,
        description="Size of message body in bytes"
    )

    # AMQP metadata
    properties: AMQPMessageProperties = Field(
        default_factory=AMQPMessageProperties,
        description="AMQP message properties"
    )
    delivery: AMQPDeliveryInfo = Field(
        ...,
        description="AMQP delivery information"
    )

    # Connector metadata
    queue: str = Field(
        ...,
        description="Queue from which message was consumed"
    )
    virtual_host: str = Field(
        default="/",
        description="Virtual host"
    )
    sequence: int = Field(
        default=0,
        description="Sequence number within session"
    )
    session_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique session identifier"
    )


# =============================================================================
# AMQP Connection Handler
# =============================================================================

class AMQPConnection:
    """
    Handles AMQP connection and message consumption.

    Note: This is a reference implementation. In production, you would use
    aio-pika or another async AMQP library.
    """

    def __init__(self, config: AMQPEndpointConfig):
        self.config = config
        self._connected = False
        self._session_id = str(uuid.uuid4())
        self._sequence = 0
        self._connection = None
        self._channel = None

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
                context.load_verify_locations(cadata=self.config.tls.ca_cert)
            else:
                context.load_verify_locations(cafile=self.config.tls.ca_cert)

        if self.config.tls.client_cert and self.config.tls.client_key:
            key_path = self.config.tls.client_key.get_secret_value()
            context.load_cert_chain(
                certfile=self.config.tls.client_cert,
                keyfile=key_path
            )

        return context

    async def connect(self) -> None:
        """
        Establish connection to AMQP broker.

        In a real implementation, this would use aio-pika or similar:

        ```python
        import aio_pika

        ssl_context = await self._create_ssl_context()
        self._connection = await aio_pika.connect_robust(
            self.config.get_amqp_url(),
            ssl_context=ssl_context,
            timeout=self.config.connection_timeout,
            heartbeat=self.config.heartbeat,
        )
        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=self.config.consumer.prefetch_count)
        ```
        """
        # Placeholder for actual AMQP connection
        self._connected = True
        self._session_id = str(uuid.uuid4())
        self._sequence = 0

    async def disconnect(self) -> None:
        """Close the AMQP connection."""
        self._connected = False
        if self._channel:
            try:
                await self._channel.close()
            except Exception:
                pass
        if self._connection:
            try:
                await self._connection.close()
            except Exception:
                pass
        self._channel = None
        self._connection = None

    async def declare_queue(self) -> None:
        """
        Declare the queue and set up bindings.

        In a real implementation:

        ```python
        queue_args = self.config.queue.arguments or {}
        if self.config.max_priority:
            queue_args['x-max-priority'] = self.config.max_priority
        if self.config.message_ttl:
            queue_args['x-message-ttl'] = self.config.message_ttl

        self._queue = await self._channel.declare_queue(
            name=self.config.queue.name,
            durable=self.config.queue.durable,
            exclusive=self.config.queue.exclusive,
            auto_delete=self.config.queue.auto_delete,
            arguments=queue_args,
        )

        for binding in self.config.bindings:
            await self._queue.bind(
                exchange=binding.exchange,
                routing_key=binding.routing_key,
                arguments=binding.arguments,
            )
        ```
        """
        pass

    async def consume_messages(self) -> AsyncIterator[AMQPDocument]:
        """
        Consume messages from the queue.

        In a real implementation:

        ```python
        async with self._queue.iterator(
            no_ack=self.config.consumer.no_ack,
            exclusive=self.config.consumer.exclusive,
            consumer_tag=self.config.consumer.consumer_tag,
            arguments={'x-priority': self.config.consumer.priority} if self.config.consumer.priority else None,
        ) as queue_iter:
            async for message in queue_iter:
                self._sequence += 1
                doc = self._message_to_document(message)
                yield doc

                if not self.config.consumer.no_ack:
                    await message.ack()
        ```
        """
        if not self._connected:
            raise RuntimeError("Not connected")

        # Placeholder: yield nothing in base implementation
        # Real implementation would iterate over queue messages
        while self._connected:
            await asyncio.sleep(1)
            # In real implementation, yield messages here
            if not self._connected:
                break

    def _parse_message_body(self, body: bytes, content_type: Optional[str] = None) -> Any:
        """Parse message body based on content type."""
        try:
            # Try JSON first if content type suggests it
            if content_type and 'json' in content_type.lower():
                return json.loads(body.decode('utf-8'))

            # Try to decode as string
            text = body.decode('utf-8')

            # Try JSON parsing
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return text

        except UnicodeDecodeError:
            # Return base64 for binary data
            import base64
            return {"_binary": True, "_base64": base64.b64encode(body).decode('ascii')}

    def _message_to_document(
            self,
            body: bytes,
            properties: Dict[str, Any],
            delivery_info: Dict[str, Any]
    ) -> AMQPDocument:
        """Convert AMQP message to document."""
        self._sequence += 1

        content_type = properties.get('content_type')
        parsed_body = self._parse_message_body(body, content_type)

        return AMQPDocument(
            received_at=datetime.now(timezone.utc),
            body=parsed_body,
            body_raw=body.decode('utf-8', errors='replace') if isinstance(parsed_body, str) else None,
            body_size=len(body),
            properties=AMQPMessageProperties(
                content_type=properties.get('content_type'),
                content_encoding=properties.get('content_encoding'),
                headers=properties.get('headers'),
                delivery_mode=properties.get('delivery_mode'),
                priority=properties.get('priority'),
                correlation_id=properties.get('correlation_id'),
                reply_to=properties.get('reply_to'),
                expiration=properties.get('expiration'),
                message_id=properties.get('message_id'),
                timestamp=properties.get('timestamp'),
                type=properties.get('type'),
                user_id=properties.get('user_id'),
                app_id=properties.get('app_id'),
            ),
            delivery=AMQPDeliveryInfo(
                consumer_tag=delivery_info.get('consumer_tag', ''),
                delivery_tag=delivery_info.get('delivery_tag', 0),
                redelivered=delivery_info.get('redelivered', False),
                exchange=delivery_info.get('exchange', ''),
                routing_key=delivery_info.get('routing_key', ''),
            ),
            queue=self.config.queue.name,
            virtual_host=self.config.virtual_host,
            sequence=self._sequence,
            session_id=self._session_id,
        )


# =============================================================================
# Capture State Management
# =============================================================================

class CaptureState(BaseModel):
    """Checkpoint state for the AMQP capture."""
    last_delivery_tag: int = 0
    last_sequence: int = 0
    last_session_id: Optional[str] = None
    last_timestamp: Optional[datetime] = None
    total_messages: int = 0
    total_bytes: int = 0
    reconnect_count: int = 0
    last_routing_key: Optional[str] = None


# =============================================================================
# Main Capture Logic
# =============================================================================

class AMQPCapture:
    """Main capture class for AMQP connector."""

    def __init__(self, config: AMQPEndpointConfig):
        self.config = config
        self.connection = AMQPConnection(config)
        self.state = CaptureState()
        self._running = False

    async def start(
            self,
            on_document: Callable[[AMQPDocument], Awaitable[None]],
            on_checkpoint: Optional[Callable[[CaptureState], Awaitable[None]]] = None
    ) -> None:
        """
        Start consuming messages from AMQP queue.

        Args:
            on_document: Async callback for each captured message
            on_checkpoint: Optional async callback for checkpoints
        """
        self._running = True
        reconnect_count = 0

        while self._running:
            try:
                await self.connection.connect()
                await self.connection.declare_queue()
                reconnect_count = 0
                self.state.reconnect_count += 1

                async for doc in self.connection.consume_messages():
                    if not self._running:
                        break

                    self.state.last_delivery_tag = doc.delivery.delivery_tag
                    self.state.last_sequence = doc.sequence
                    self.state.last_session_id = doc.session_id
                    self.state.last_timestamp = doc.received_at
                    self.state.total_messages += 1
                    self.state.total_bytes += doc.body_size
                    self.state.last_routing_key = doc.delivery.routing_key

                    await on_document(doc)

                    # Periodic checkpoint
                    if self.state.total_messages % 1000 == 0 and on_checkpoint:
                        await on_checkpoint(self.state)

            except asyncio.CancelledError:
                break
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
        "configSchema": AMQPEndpointConfig.model_json_schema(),
        "resourceConfigSchema": AMQPResourceConfig.model_json_schema(),
        "documentationUrl": "https://docs.estuary.dev/reference/connectors/source-amqp/",
    }


async def discover(config: Dict[str, Any]) -> Dict[str, Any]:
    """Discover available streams/bindings."""
    endpoint_config = AMQPEndpointConfig(**config)

    queue_name = endpoint_config.queue.name

    return {
        "bindings": [
            {
                "recommendedName": f"amqp_{queue_name}".replace(".", "_").replace("-", "_"),
                "resourceConfig": {
                    "stream": f"amqp_{queue_name}"
                },
                "documentSchema": AMQPDocument.model_json_schema(),
                "key": ["/_meta/received_at", "/delivery/delivery_tag", "/session_id"],
            }
        ]
    }


async def validate(config: Dict[str, Any]) -> Dict[str, Any]:
    """Validate the configuration by attempting a connection."""
    endpoint_config = AMQPEndpointConfig(**config)
    connection = AMQPConnection(endpoint_config)

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
    endpoint_config = AMQPEndpointConfig(**config)
    capture_state = CaptureState(**(state or {}))

    amqp_capture = AMQPCapture(endpoint_config)
    document_queue: asyncio.Queue = asyncio.Queue()

    async def on_document(doc: AMQPDocument):
        await document_queue.put(("document", doc.model_dump()))

    async def on_checkpoint(state: CaptureState):
        await document_queue.put(("checkpoint", state.model_dump()))

    # Start capture in background
    capture_task = asyncio.create_task(
        amqp_capture.start(on_document, on_checkpoint)
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
                if capture_task.done():
                    exc = capture_task.exception()
                    if exc:
                        raise exc
                    break

    finally:
        await amqp_capture.stop()
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
        print("Usage: source-amqp <command> [args]")
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