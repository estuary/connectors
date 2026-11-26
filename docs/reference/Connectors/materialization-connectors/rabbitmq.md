---
description: Publish Estuary collection documents to RabbitMQ exchanges with persistent JSON messages and configurable routing keys.
---

# RabbitMQ

This connector materializes Estuary collections as messages in RabbitMQ exchanges
using AMQP 0-9-1. It supports direct, fanout, and topic exchanges.

## Prerequisites

To use this connector, you'll need:

* A RabbitMQ server reachable from Estuary on its AMQP port.
* A username and password with access to the virtual host and permission to
  check the destination exchanges and publish to them.
* Queues and bindings configured to receive messages from those exchanges.
* At least one Estuary collection to materialize.

Declare each destination exchange and bind your queues before publishing the
materialization. Validate checks that every exchange exists using a passive
declaration and fails if one is missing. Apply repeats this check. The connector
does not create or change exchanges, queues, or bindings. Existing exchanges
retain their settings and arguments.

:::warning
RabbitMQ acknowledges and permanently discards unroutable messages unless the
exchange has an alternate exchange. An existing exchange alone does not ensure
messages will reach a queue; bind queues before starting the initial backfill.
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
| **`/exchange`** | Exchange | Name of an existing exchange to publish to. Bind queues before publishing. The unnamed default exchange is not supported. Maximum 255 bytes. | string | Required |
| `/exchangeType` | Exchange Type | Expected exchange type: `direct`, `fanout`, or `topic`. Passive checks verify existence only; they do not check or change the type. | string | `topic` |
| `/routingKey` | Routing Key | Static routing key and fallback for `routingKeyField`. Maximum 255 bytes. | string | `""` |
| `/routingKeyField` | Routing Key Field | JSON pointer to a projected collection field whose value supplies the routing key. | string | Optional |

Each binding is identified by its exchange. Use a separate exchange for each
binding in a materialization. Changing `routingKey` or `routingKeyField` preserves
the binding identity and does not trigger a backfill; subsequent messages use the
new routing configuration.

:::warning
Changing a binding's `exchange` makes Estuary treat it as a new binding and
re-publish the collection's full history. Set `notBefore` under the binding's
`source` when changing exchanges if you want to exclude that history.
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
as `/status`, rather than the projection's field name. That projection must be
included in field selection. A string at that location is used directly,
including an empty string. Numbers and booleans use their JSON
representation, such as `42` or `true`. Flow applies projection defaults before
supplying the selected field. Missing values without a default, nulls, arrays,
and objects fall back to `routingKey`. A resulting routing key over 255 bytes
fails the transaction.

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
| `message_id` | Lowercase hexadecimal SHA-256 of `<packed-key>\|<uuid>` |

For `message_id`, `<packed-key>` is the binary collection key supplied by the
Flow runtime, including any projection defaults. The separator is a literal `|`
byte, and `<uuid>` is the UTF-8 string from the document's `/_meta/uuid`, without
JSON quotes. Replaying the same event after a restart produces the same message
ID; an update with a new UUID produces a new ID.

The connector uses publisher confirms and waits for every message to be confirmed
before completing a transaction. A negative acknowledgement or a closed publish
channel fails the transaction. A confirmation acknowledges acceptance by RabbitMQ;
it does not indicate that a consumer processed the message.

## Limitations

* One binding per exchange; to publish two collections to one exchange with
  different routing keys, use two materializations.
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
