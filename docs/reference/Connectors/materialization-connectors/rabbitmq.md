---
description: Publish Estuary collection documents to RabbitMQ exchanges with persistent JSON messages and configurable routing keys.
---

# RabbitMQ

This connector materializes Estuary collections as messages in RabbitMQ exchanges
using AMQP 0-9-1. It supports direct, fanout, and topic exchanges.

## Prerequisites

To use this connector, you'll need:

* A RabbitMQ server reachable from Estuary on its AMQP port.
* A username and password with access to the virtual host, configure permission
  to declare the destination exchanges, and write permission to publish to them.
* Queues and bindings configured to receive messages from those exchanges.
* At least one Estuary collection to materialize.

The connector creates missing exchanges as durable, non-auto-delete, non-internal
exchanges without arguments. Existing exchanges retain their type, settings, and
arguments. The connector does not create queues or bindings.

:::warning
An exchange created by the connector has no bound queues. RabbitMQ acknowledges
and permanently discards messages published before a queue is bound, including
the initial backfill. Declare each exchange yourself and bind your queues to it,
or give it an alternate exchange, before publishing the materialization.
:::

## Configuration

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/host`** | Host | Hostname or IP address of the RabbitMQ server, without a scheme or port. | string | Required |
| `/port` | Port | AMQP port. For TLS, set this to the server's TLS port, typically `5671`. | integer | `5672` |
| `/virtualHost` | Virtual Host | RabbitMQ virtual host containing the exchanges. | string | `/` |
| **`/username`** | Username | Username for authentication. | string | Required |
| **`/password`** | Password | Password for authentication. Stored as a secret. | string | Required |
| `/tls` | Use TLS | Enable TLS using the system's trusted CA certificates. Does not change the port. | boolean | `false` |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/exchange`** | Exchange | Name of the exchange to publish to. The unnamed default exchange is not supported. Maximum 255 bytes. | string | Required |
| `/exchangeType` | Exchange Type | Type to use when creating a missing exchange: `direct`, `fanout`, or `topic`. Does not change an existing exchange. | string | `topic` |
| `/routingKey` | Routing Key | Static routing key and fallback for `routingKeyField`. Maximum 255 bytes. | string | `""` |
| `/routingKeyField` | Routing Key Field | JSON pointer to a projected collection field whose value supplies the routing key. | string | Optional |

Each binding is identified by its exchange and, when non-empty, its static
`routingKey`. Bindings sharing an exchange must have distinct static routing keys.
Changing only `routingKeyField` does not give a binding a distinct identity.
Bindings sharing an exchange must also use the same `exchangeType`.

:::warning
Changing a binding's `exchange` or static `routingKey`, including adding or clearing
a `routingKey`, makes Estuary treat it as a new binding. It then re-publishes the
collection's full history to the exchange. To avoid this, set `notBefore` under the
binding's `source` when you change either field.
:::

### Sample

```yaml
materializations:
  acmeCo/rabbitmq:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-rabbitmq:v1
        config:
          host: rabbitmq.example.com
          port: 5671
          virtualHost: /
          username: estuary
          password: example-password
          tls: true
    bindings:
      - resource:
          exchange: orders
          exchangeType: topic
          routingKey: orders
          routingKeyField: /status
        source: acmeCo/orders
```

### Routing keys

`routingKeyField` must match a JSON pointer in the collection's projections, such
as `/status`, rather than the projection's field name. A string at that location
is used directly, including an empty string. Numbers and booleans use their JSON
representation, such as `42` or `true`. Missing values, nulls, arrays, and objects
fall back to `routingKey`. A resulting routing key over 255 bytes fails the
transaction.

Deletion documents usually contain only the collection key and `_meta`, so the
`routingKeyField` value is often missing and they are published with the static
`routingKey`. Consumers that must see deletions should bind that key, or `#` on a
topic exchange. Set a non-empty `routingKey` or add an alternate exchange so
deletions are not dropped as unroutable.

Direct exchanges match queue bindings exactly. Topic exchanges match routing key
patterns. Fanout exchanges deliver to every bound queue regardless of routing key.

## Messages and delta updates

This connector supports only [delta updates](/concepts/materialization/#delta-updates).
Each stored document is published as a new message containing the full document
as compact JSON. Updates append messages; they do not replace previous messages.
Deletions also append messages and preserve `_meta.op: "d"` in the body for
consumers to process.

Messages have these properties, with no additional headers or properties:

| Property | Value |
|---|---|
| `content_type` | `application/json` |
| `delivery_mode` | `2` (persistent) |
| `message_id` | Lowercase hexadecimal SHA-256 of `<key>\|<uuid>` |

For `message_id`, `<key>` is the collection key values in collection key order as
a compact JSON array, such as `[1]` or `["us",7]`. An absent key field uses its
projection default, or JSON `null` when no default is defined. `<uuid>` is the
plain string from the document's `/_meta/uuid`, without JSON quotes. Replaying the
same event produces the same message ID; an update with a new UUID produces a new ID.

The connector uses publisher confirms and waits for every message to be confirmed
before completing a transaction. A negative acknowledgement or a closed publish
channel fails the transaction. A confirmation acknowledges acceptance by RabbitMQ;
it does not indicate that a consumer processed the message.

## Limitations

* Delivery is at-least-once (consumers dedupe on `message_id`), and RabbitMQ drops
  unroutable messages unless the exchange has an alternate exchange.
* A replay after a failed transaction can group updates differently, so it can
  re-deliver an older state of a key after a newer one under a new `message_id`.
  Consumers that keep the latest state per key should apply a message only when its
  `/_meta/uuid` timestamp (UUID v1) is newer than the last one applied for that key.
* A document larger than the broker's `max_message_size` (16 MiB by default on
  RabbitMQ 4.x) fails the transaction, and the materialization stops until the
  broker limit is raised or the document shrinks.
* The connector publishes with `mandatory=false` and does not handle returned messages.
* Standard updates, `hardDelete`, the default exchange, and queue creation are not supported.
* Custom CA certificates, mutual TLS, SSH tunnels, and OAuth are not supported.
* A materialization must run as a single shard.
