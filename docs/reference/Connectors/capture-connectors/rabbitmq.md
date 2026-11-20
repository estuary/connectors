---
description: Capture RabbitMQ messages into Estuary collections from classic, quorum, and stream queues, with acknowledgements after commit and resumable stream offsets.
---

# RabbitMQ

This connector captures messages from RabbitMQ queues into Estuary collections. It supports **classic, quorum, and stream queues** using AMQP 0-9-1.

## Prerequisites

- One or more existing queues in a RabbitMQ virtual host.
- A RabbitMQ user with access to that virtual host and read permission on each captured queue, authenticated with a password or an x509 client certificate.
- Network access from Estuary to the AMQP listener and the management HTTP API on the same host.
- The RabbitMQ management plugin enabled, and a user with the `management` tag or another tag that permits management API access, for queue discovery. Discovery uses the endpoint username and password, including when AMQP uses certificate authentication.

The connector reads existing queues and does not create queues, exchanges, or bindings. The management API is used only during discovery; validation and capture use AMQP.

## Queue behavior

### Classic and quorum queues

The connector limits the number of unacknowledged messages to `prefetchCount` per queue, which defaults to 10. Increasing this value can improve throughput when commit latency is limiting; size it for the task's memory allocation and message sizes.

Buffered message bodies can use approximately `prefetchCount × maximum message size × number of queues` of memory. On 64-bit workers, the acknowledgement buffer also reserves about `prefetchCount × 32 bytes × number of queues`, even without traffic. Allow additional memory for message headers, client buffers, and document serialization. Emitted bodies and headers are released while awaiting a commit. Large messages or many bindings can require a larger task allocation. RabbitMQ may impose a lower prefetch limit for a queue type.

:::warning
This capture consumes classic and quorum queues. If another consumer shares a queue with Estuary, messages are divided between the consumers. To deliver every message to Estuary and another application, bind a separate queue for each consumer to the same exchange.
:::

Already acknowledged messages cannot be read again by resetting or recreating a capture. They must be republished if you need to capture them again.

#### Delivery guarantees

Messages are acknowledged and removed from the queue only after Estuary commits them. Capture is at least once: after a restart or disconnect, messages that were captured but not yet acknowledged are delivered again and captured again.

With the default collection key (`_meta.id`), a repeat of a message that has a producer message ID overwrites the earlier document in standard (non-delta) materializations, so producers should set a unique message ID on each message. Messages without a message ID get a new UUID each time they are captured, so their repeats appear as separate documents. `_meta.redelivered` marks deliveries that RabbitMQ flagged as redelivered.

### Stream queues

Stream captures leave messages in RabbitMQ. The connector saves the next offset for each queue alongside the captured documents and resumes from the committed offset on subsequent runs. Streams also use the endpoint's `prefetchCount` to bound unacknowledged deliveries.

The binding's optional `streamStartOffset` controls where a stream starts when it has no saved offset:

| Value | Initial position |
|---|---|
| `first` | First retained message; the default. |
| `last` | Beginning of the last chunk in the stream, which can contain multiple messages. |
| `next` | New messages arriving after the consumer attaches. |
| RFC3339 timestamp, such as `2026-11-01T00:00:00Z` | Position selected by RabbitMQ for that time. Timestamps must be on or after 1970; fractional seconds are truncated to whole seconds. |
| Interval, such as `7D` or `12h` | Position selected by RabbitMQ relative to its current time. Use a positive integer followed by `Y` (years), `M` (months), `D` (days), `h` (hours), `m` (minutes), or `s` (seconds). |

Timestamp and interval positions use RabbitMQ's stream storage timestamps, rather than the producer's message timestamp. Selection operates at chunk granularity and can include messages earlier than the requested time. Saved offsets always take precedence over `streamStartOffset`; changing this setting does not rewind an existing capture. Before the first message checkpoint is committed, reconnecting reevaluates the initial position, so `next`, `last`, and relative intervals may select a later position.

Configure stream retention to keep messages long enough for the capture to read them, including during outages. Messages that RabbitMQ has removed under its retention policy are no longer available for capture. Resetting a stream binding applies its configured initial position again. `streamStartOffset` has no effect on classic or quorum queues.

Saved numeric offsets request an exact message position, unlike timestamp and interval positions that select chunks. If RabbitMQ delivers an offset below the saved next offset, capture stops with the queue name and both offsets, leaving the saved position unchanged. This can indicate a recreated stream or a broker restored to a shorter log. Backfill the binding into a new collection, using `streamStartOffset: first` to capture retained messages. A new collection prevents reused `<queue>:<offset>` IDs from overwriting documents from the old stream. The connector does not automatically rewind, and offsets alone cannot identify every stream reset.

## Captured document structure

If the body is a single JSON object, its properties become the document's top-level fields. Other bodies, including JSON arrays, scalars, and plain text, are captured as a string in `body`. Bodies that are not valid UTF-8 are base64 encoded. The connector replaces any `_meta` property in the message body with its own metadata.

| Field | Description |
|---|---|
| `_meta.queue` | Name of the queue. |
| `_meta.id` | Document identifier: `<queue>:<offset>` for stream queues; otherwise the message ID supplied by the producer, or a generated UUID when no message ID is present. |
| `_meta.exchange` | Exchange the message was published to; an empty string identifies the default exchange. |
| `_meta.routingKey` | Routing key used to publish the message. |
| `_meta.timestamp` | Producer-supplied timestamp in RFC3339 format, if present and its UTC year is between 1 and 9999. Out-of-range timestamps are omitted. |
| `_meta.messageId` | Message ID supplied by the producer, if present. |
| `_meta.redelivered` | Whether RabbitMQ marked this delivery as a redelivery. |
| `_meta.headers` | Message headers, if present; byte-array headers become base64 strings. String values and header names use the reversible encoding below when needed. Timestamp headers outside UTC years 1–9999 become Unix seconds, and non-finite numbers become `"NaN"`, `"Infinity"`, or `"-Infinity"`, including within nested tables and arrays. |
| `_meta.offset` | Integer offset within the queue, for stream queues only. |
| `_meta.contentType` | Content type supplied by the producer. |
| `_meta.contentEncoding` | Content encoding supplied by the producer; the connector does not decompress or decode the body based on this property. |
| `_meta.correlationId` | Correlation ID supplied by the producer. |
| `_meta.replyTo` | Reply destination supplied by the producer. |
| `_meta.type` | Message type supplied by the producer. |
| `_meta.appId` | Application ID supplied by the producer. |
| `_meta.userId` | User ID supplied by the producer. |
| `_meta.priority` | Integer message priority, including zero. |
| `_meta.deliveryMode` | Integer delivery mode: `1` for non-persistent or `2` for persistent messages; `0` if unspecified. |
| `_meta.expiration` | Per-message expiration in milliseconds, preserved as the AMQP string value. |

Empty string properties are omitted. Priority and delivery mode are always included; the AMQP client reports zero for an unspecified numeric property.

AMQP string properties, header names, and string header values preserve their original bytes. Invalid UTF-8 becomes `~base64:` followed by standard base64 of the original bytes. Valid strings beginning with the reserved prefix `~base64:` are encoded the same way, so a literal string cannot collide with encoded bytes. Decode the suffix exactly once to recover the original value. This applies recursively to headers and to `_meta.id` when it uses a producer message ID; ordinary UTF-8 strings are unchanged. Body strings and byte-array headers retain their plain base64 representation.

### Discovery and collection keys

Discovery proposes one binding per supported queue in the configured virtual host, with the queue name as its recommended name. Queue types other than classic, quorum, and stream are skipped. Server-named queues whose names start with `amq.gen-` are excluded when they are exclusive or auto-delete queues.

Discovery records each queue's type in its binding so capture does not depend on broker error messages to distinguish streams. Bindings without a `queueType` use a compatibility probe; on brokers that accept stream arguments for other queue types, rediscover the queues or set `queueType` explicitly.

Discovered collections use `/_meta/id` as their key. For classic and quorum queues, producers should assign a unique message ID to each message. Reusing a message ID gives different messages the same collection key. If multiple queues feed the same collection, their message IDs should also be unique across those queues.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file. See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/host`** | Host | Hostname or IP address of the RabbitMQ server, without a scheme or port. | string | Required |
| `/port` | Port | AMQP listener port. | integer | `5672` |
| `/virtualHost` | Virtual Host | Virtual host containing the queues. | string | `/` |
| `/username` | Username | RabbitMQ username for PLAIN authentication and management API discovery. | string | Required for PLAIN and discovery |
| `/password` | Password | RabbitMQ password. This field is a secret. | string | Required for PLAIN and discovery |
| `/tls` | TLS | Use TLS for AMQP and HTTPS for discovery. | boolean | `false` |
| `/managementPort` | Management Port | Management HTTP API port, used only for discovery. | integer | `15672` |
| `/authMechanism` | Authentication Mechanism | AMQP SASL mechanism: `PLAIN` or `EXTERNAL`. | string | `PLAIN` |
| `/tlsConfig/ssl_server_ca` | SSL Server CA | PEM-encoded CA bundle for AMQP and HTTPS server verification. This field is a secret. | string | System roots |
| `/tlsConfig/ssl_client_cert` | SSL Client Certificate | PEM-encoded client certificate chain for mutual TLS. This field is a secret. | string | Required with client key or EXTERNAL |
| `/tlsConfig/ssl_client_key` | SSL Client Key | PEM-encoded private key for the client certificate. This field is a secret. | string | Required with client certificate or EXTERNAL |
| `/prefetchCount` | Prefetch Count | Maximum unacknowledged messages per queue, from 1 to 65535. | integer | `10` |

Enabling TLS does not change either port automatically. Set both ports to your server's TLS listener ports, commonly `5671` for AMQP and `15671` for the management API. The server certificates must be trusted by the connector and valid for the configured host.

##### Private CAs and certificate authentication

Set `tls: true` and provide `tlsConfig.ssl_server_ca` to trust an internal CA or a self-signed server certificate. The supplied bundle replaces system roots for both AMQP and HTTPS discovery, so include every CA needed by the two listeners. Hostname verification remains enabled. Omit the CA bundle to retain system-root trust.

For mutual TLS, provide both `ssl_client_cert` and `ssl_client_key`. Use a PEM certificate chain with the client certificate first and an unencrypted PEM private key. These settings also apply to HTTPS discovery. PLAIN authentication can be used together with mutual TLS.

To authenticate AMQP using the certificate identity, set `authMechanism: EXTERNAL`. Enable RabbitMQ's `rabbitmq_auth_mechanism_ssl` plugin, configure its certificate-to-user mapping, and grant the mapped user access to the virtual host and queues. EXTERNAL requires TLS and a client certificate/key pair. Username and password may be omitted for validation and capture with manually configured bindings, but discovery still requires management API username/password authentication.

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/queue`** | Queue | Name of an existing queue in the configured virtual host. | string | Required |
| `/queueType` | Queue Type | Queue type recorded by discovery: `classic`, `quorum`, or `stream`; if omitted, the connector probes for stream support. | string | Optional |
| `/streamStartOffset` | Stream Start Offset | Initial stream position: `first`, `last`, `next`, an RFC3339 timestamp, or an interval. Saved offsets take precedence. | string | `first` |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-rabbitmq:v1
        config:
          host: rabbitmq.example.com
          port: 5671
          virtualHost: /
          username: estuary
          password: <secret>
          tls: true
          managementPort: 15671
          prefetchCount: 500
    bindings:
      - resource:
          queue: orders
          queueType: classic
        target: ${PREFIX}/orders
      - resource:
          queue: events
          queueType: stream
          streamStartOffset: 7D
        target: ${PREFIX}/events
```

For certificate authentication, add the following endpoint settings while keeping management credentials if you use discovery:

```yaml
tls: true
authMechanism: EXTERNAL
tlsConfig:
  ssl_server_ca: |
    <PEM CA bundle>
  ssl_client_cert: |
    <PEM client certificate chain>
  ssl_client_key: |
    <PEM private key>
```
