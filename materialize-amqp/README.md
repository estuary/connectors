# Source AMQP Connector

An Estuary Flow source connector for capturing messages from AMQP-compatible message brokers like RabbitMQ.

## Overview

This connector captures streaming messages from AMQP 0-9-1 compatible brokers with support for:

- **Multiple authentication methods**: Plain, AMQPLAIN, External (certificate-based)
- **TLS/SSL** encrypted connections with mutual TLS support
- **Exchange bindings** with routing key patterns
- **Message priority** queues and consumer priorities
- **Automatic reconnection** with configurable retry logic
- **Checkpointing** for exactly-once semantics

## Configuration

### Endpoint Configuration

#### Connection Settings

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `host` | string | No | "localhost" | AMQP broker hostname |
| `port` | integer | No | 5672 | Broker port (5671 for TLS) |
| `virtual_host` | string | No | "/" | Virtual host |
| `username` | string | No | "guest" | Username for authentication |
| `password` | string | No | "guest" | Password for authentication |
| `auth_type` | string | No | "plain" | Auth mechanism: `plain`, `amqplain`, `external` |

#### TLS Settings

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `tls.enabled` | boolean | No | false | Enable TLS/SSL (amqps://) |
| `tls.verify_cert` | boolean | No | true | Verify server certificate |
| `tls.ca_cert` | string | No | - | CA certificate file or PEM string |
| `tls.client_cert` | string | No | - | Client certificate for mTLS |
| `tls.client_key` | string | No | - | Client private key for mTLS |

#### Queue Configuration

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `queue.name` | string | Yes | - | Queue name to consume from |
| `queue.durable` | boolean | No | true | Queue survives broker restart |
| `queue.exclusive` | boolean | No | false | Exclusive to this connection |
| `queue.auto_delete` | boolean | No | false | Delete when consumer disconnects |
| `queue.arguments` | object | No | - | Additional x-arguments |

#### Exchange Bindings

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `bindings[].exchange` | string | Yes | - | Exchange name to bind to |
| `bindings[].routing_key` | string | No | "#" | Routing key pattern |
| `bindings[].arguments` | object | No | - | Binding arguments |

#### Consumer Settings

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `consumer.prefetch_count` | integer | No | 100 | Messages to prefetch |
| `consumer.no_ack` | boolean | No | false | Disable acknowledgments |
| `consumer.exclusive` | boolean | No | false | Exclusive consumer access |
| `consumer.priority` | integer | No | - | Consumer priority (0-255) |

#### Message Settings

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `message_ttl` | integer | No | - | Message TTL in milliseconds |
| `max_priority` | integer | No | - | Enable priority queue (0-255) |
| `heartbeat` | integer | No | 60 | Heartbeat interval in seconds |

#### Connection Management

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `connection_timeout` | float | No | 30.0 | Connection timeout in seconds |
| `reconnect_delay` | float | No | 5.0 | Reconnection delay in seconds |
| `max_reconnect_attempts` | integer | No | -1 | Max reconnect attempts (-1 = unlimited) |

### Example Configurations

#### Basic RabbitMQ Connection

```yaml
captures:
  acmeCo/rabbitmq-events:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-amqp:dev
        config:
          host: rabbitmq.example.com
          port: 5672
          username: myuser
          password: mypassword
          virtual_host: /production
          queue:
            name: events
            durable: true
    bindings:
      - resource:
          stream: events
        target: acmeCo/events
```

#### With Exchange Binding

```yaml
config:
  host: rabbitmq.example.com
  queue:
    name: order_events
    durable: true
  bindings:
    - exchange: orders
      routing_key: "orders.created.#"
    - exchange: orders
      routing_key: "orders.updated.#"
```

#### TLS Connection

```yaml
config:
  host: secure-rabbitmq.example.com
  port: 5671
  tls:
    enabled: true
    verify_cert: true
    ca_cert: /path/to/ca.crt
  queue:
    name: secure_events
```

#### Mutual TLS (mTLS)

```yaml
config:
  host: mtls-rabbitmq.example.com
  port: 5671
  auth_type: external
  tls:
    enabled: true
    verify_cert: true
    ca_cert: /path/to/ca.crt
    client_cert: /path/to/client.crt
    client_key: /path/to/client.key
  queue:
    name: mtls_events
```

#### Priority Queue

```yaml
config:
  host: rabbitmq.example.com
  queue:
    name: priority_tasks
    durable: true
  max_priority: 10
  consumer:
    prefetch_count: 50
    priority: 5  # Higher priority consumer
```

#### High-Throughput Configuration

```yaml
config:
  host: rabbitmq.example.com
  queue:
    name: high_volume_events
    durable: true
  consumer:
    prefetch_count: 500
    no_ack: false
  heartbeat: 30
  reconnect_delay: 1.0
```

## Document Schema

Each captured message includes:

| Field | Type | Description |
|-------|------|-------------|
| `received_at` | datetime | When message was received |
| `body` | any | Parsed message body (JSON or string) |
| `body_raw` | string | Raw body as string |
| `body_size` | integer | Size in bytes |
| `properties.content_type` | string | MIME content type |
| `properties.headers` | object | Message headers |
| `properties.priority` | integer | Message priority |
| `properties.correlation_id` | string | Correlation ID |
| `properties.reply_to` | string | Reply-to routing key |
| `properties.message_id` | string | Message ID |
| `properties.timestamp` | datetime | Message timestamp |
| `properties.type` | string | Message type |
| `properties.app_id` | string | Application ID |
| `delivery.consumer_tag` | string | Consumer tag |
| `delivery.delivery_tag` | integer | Delivery tag |
| `delivery.redelivered` | boolean | Was redelivered |
| `delivery.exchange` | string | Source exchange |
| `delivery.routing_key` | string | Routing key used |
| `queue` | string | Queue name |
| `virtual_host` | string | Virtual host |
| `sequence` | integer | Sequence in session |
| `session_id` | string | Session identifier |

### Example Document

```json
{
  "received_at": "2024-01-15T10:30:00Z",
  "body": {
    "order_id": "ORD-12345",
    "customer": "john@example.com",
    "total": 99.99
  },
  "body_size": 78,
  "properties": {
    "content_type": "application/json",
    "priority": 5,
    "message_id": "msg-abc123",
    "correlation_id": "corr-xyz789",
    "headers": {
      "x-retry-count": 0
    }
  },
  "delivery": {
    "consumer_tag": "ctag-1",
    "delivery_tag": 42,
    "redelivered": false,
    "exchange": "orders",
    "routing_key": "orders.created.us"
  },
  "queue": "order_events",
  "virtual_host": "/production",
  "sequence": 1,
  "session_id": "abc123-def456"
}
```

## Routing Keys

### Direct Exchange

For direct exchanges, the routing key must exactly match the binding key:

```yaml
bindings:
  - exchange: direct_exchange
    routing_key: "order.created"
```

### Topic Exchange

Topic exchanges support wildcards:
- `*` matches exactly one word
- `#` matches zero or more words

```yaml
bindings:
  - exchange: topic_exchange
    routing_key: "orders.*.us"     # orders.created.us, orders.updated.us
  - exchange: topic_exchange
    routing_key: "orders.#"        # All order events
```

### Fanout Exchange

Fanout exchanges ignore routing keys:

```yaml
bindings:
  - exchange: fanout_exchange
    routing_key: ""  # Ignored for fanout
```

### Headers Exchange

Headers exchanges use message headers for routing:

```yaml
bindings:
  - exchange: headers_exchange
    arguments:
      x-match: all
      region: us
      priority: high
```

## Best Practices

### Message Acknowledgment

Always use message acknowledgment (`no_ack: false`) in production to ensure messages are not lost:

```yaml
consumer:
  no_ack: false
  prefetch_count: 100
```

### Prefetch Count

Tune `prefetch_count` based on message processing time:
- Lower values (10-50) for slow processing
- Higher values (100-500) for fast processing

### Durable Queues

Use durable queues for important data:

```yaml
queue:
  durable: true
  auto_delete: false
```

### Priority Queues

Only enable priority if needed (has performance impact):

```yaml
max_priority: 10  # Only if messages have different priorities
```

## Development

### Running Tests

```bash
pip install -e ".[dev]"
pytest
```

### Running with Docker Compose (RabbitMQ)

```yaml
version: '3'
services:
  rabbitmq:
    image: rabbitmq:3-management
    ports:
      - "5672:5672"
      - "15672:15672"
    environment:
      RABBITMQ_DEFAULT_USER: guest
      RABBITMQ_DEFAULT_PASS: guest
```

### Building Docker Image

```bash
docker build -t source-amqp:dev .
```

## License

Dual licensed under MIT or Apache 2.0 at your discretion.