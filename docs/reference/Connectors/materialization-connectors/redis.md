---
description: Materialize Estuary collections into Redis strings or RedisJSON, with SSH tunneling, custom TLS, Sentinel failover, safe key encoding, expiration, and hard deletes.
---

# Redis

This connector materializes Estuary collections into Redis strings or RedisJSON. Each key
holds the full collection document as compact JSON, making the data available
for caches and application lookups.

## Prerequisites

You'll need:

- A standalone or Sentinel-managed Redis deployment reachable from Estuary,
  directly or through an SSH bastion. Redis Cluster is not supported; managed
  services must expose a non-cluster endpoint.
- Permission to run `PING`, `MGET`, `SET`, `SCAN`, and `UNLINK` on the destination,
  plus `DEL` when hard deletes are enabled. If authentication is required,
  provide the password and, for an ACL user, the username. Selecting a nonzero
  database requires permission to run `SELECT`.
- RedisJSON bindings additionally require `JSON.MGET`, `JSON.SET`, `EXPIRE`, and
  `PERSIST`, plus `MULTI` and `EXEC` for atomic document and TTL writes.
  Use Redis 8 with JSON enabled, Redis Stack, or another deployment
  with RedisJSON support. The connector does not create search indexes.
- Sentinel connections need permission for `SENTINEL GET-MASTER-ADDR-BY-NAME`,
  `SENTINEL SENTINELS`, and `SUBSCRIBE` on Sentinel. Its credentials may differ
  from the Redis credentials.
- If your network restricts incoming connections, [allowlist the Estuary IP
  addresses](/reference/allow-ip-addresses).

Enable `tls` when your Redis endpoint requires encrypted connections. The
connector verifies the server certificate using system certificate authorities
unless a custom CA is configured.

### Private networks and TLS

Set `networkTunnel.sshForwarding.sshEndpoint` to `ssh://user@bastion[:port]` and
`privateKey` to the PEM-encoded SSH private key. The bastion must support TCP
forwarding and be able to resolve and reach the Redis endpoint. For Sentinel,
it must also reach all Sentinel and primary addresses returned by discovery;
the connector opens forwards for newly discovered addresses as needed.

Keep `address` set to the remote address. TLS verifies that remote hostname
through the tunnel. Under `advanced`, use `ssl_server_ca` for a PEM CA bundle
(for example, the CA supplied by Memorystore), and supply both
`ssl_client_cert` and `ssl_client_key` for mutual TLS. A supplied CA bundle
replaces the system trust store. `ssl_server_name` overrides the certificate
hostname and TLS SNI name. `ssl_skip_verify` disables certificate and hostname
verification and should only be used when you accept the risk of server
impersonation. All these options require `tls: true`.

### Sentinel

To discover the current primary and reconnect after failover, set `address` to
a Sentinel endpoint (usually port `26379`) and set `sentinel.masterName` to its
monitored primary name. Add other seed endpoints in `sentinel.addresses`.
The top-level `username`, `password`, and `database` apply to Redis;
`sentinel.username` and `sentinel.password` apply only to Sentinel.
TLS settings apply to both Sentinel and Redis. Without a server-name override,
each certificate must match the address discovered for that server. With an
override, all endpoints must present certificates valid for that name.

```yaml
address: sentinel-1.internal:26379
sentinel:
  masterName: mymaster
  addresses:
    - sentinel-2.internal:26379
networkTunnel:
  sshForwarding:
    sshEndpoint: ssh://flow@bastion.example.com:22
    privateKey: <SSH private key>
```

## Data model

Each binding writes keys in this form:

```text
<keyPrefix>:<key1>:<key2>:...
```

Key values follow the order of the Estuary collection's key. Strings are used
as-is; numbers and booleans use their JSON representation. For example:

| Key prefix | Collection key | Document | Redis key |
| ---------- | -------------- | -------- | --------- |
| `orders` | `[/id]` | `{"id":1,"status":"new"}` | `orders:1` |
| `events` | `[/region, /seq]` | `{"region":"us","seq":7}` | `events:us:7` |

The value is the complete document, including its `_meta` metadata. Individual
fields are not stored as Redis hash fields. The connector creates keys as
documents arrive; configuring a binding does not create keys in advance.

Choose a distinct prefix for each binding and reserve its keys for this
materialization. Prefixes must not nest: `orders` and `orders:archive` cannot
coexist because a backfill of `orders` also matches `orders:archive:*`.
Validation checks this rule for standard and delta-update bindings, including
bindings disabled or removed in the same publication. Estuary retains disabled
and previously removed bindings internally, so their prefixes remain reserved
to protect their existing keys even when they no longer appear in your catalog.
Reusing the exact prefix is allowed, but a new prefix must not be a parent or
child of a reserved prefix. If validation reports a conflict with a disabled or
removed binding, choose a non-nested prefix, such as `orders-current` instead
of `orders` when `orders:archive` is reserved. Apply also checks for overlaps
before clearing any keys. Existing values at those keys must contain valid JSON
documents that match the collection schema. Writes replace any existing value
at the same key.

The default `keyEncoding: legacy` does not escape key components. In a composite
key, values `a:b` and `c` produce
the same Redis key as values `a` and `b:c`. Likewise, the string `"1"` and the
number `1` produce the same component. To prevent these collisions, set
`keyEncoding: json`. The key suffix becomes a compact JSON array of the entire
key tuple, preserving types, component boundaries, and string escaping:

| Collection key values | Redis key with prefix `orders` |
| --------------------- | ------------------------------ |
| `1` (number) | `orders:[1]` |
| `"1"` (string) | `orders:["1"]` |
| `"a:b"`, `"c"` | `orders:["a:b","c"]` |
| `"a"`, `"b:c"` | `orders:["a","b:c"]` |
| `"a&b<c>"` | `orders:["a&b<c>"]` |

The array adds no whitespace between elements or trailing newline. Within
strings, valid UTF-8 is written as-is, including `&`, `<`, `>`, U+2028, and
U+2029, with no HTML escaping.
Only the quote, backslash, and U+0000–U+001F control characters are escaped:
`\"`, `\\`, `\b`, `\f`, `\n`, `\r`, `\t`, or lowercase `\u00xx`.

Numbers follow Go's `encoding/json` formatting. Integers use full-precision
base-10 digits. Floating-point values use the shortest round-tripping
representation without a trailing `.0`; nonzero magnitudes below `1e-6` or at
least `1e21` use exponent notation, such as `1e-7` or `1e+21`. Values supplied
as `json.Number` retain their numeric spelling. Applications must use the same
number formatting and preserve large integer precision when rebuilding keys.

Existing configurations retain legacy encoding. With legacy encoding, use
collection keys and prefixes that cannot collide, or derive an unambiguous
string key before materializing.

### RedisJSON and search

Set a binding's `format` to `json` to write the full document with
`JSON.SET key $ document`. Standard updates load documents with
`JSON.MGET key... .`; the legacy root path returns each full document without
an array wrapper. Missing keys are treated as absent. Delta updates skip loads.
Hard deletes remove the entire key with `DEL` for either format.

RedisJSON documents can be read by path and indexed by Redis Query Engine
(RediSearch). Create and manage indexes separately, for example:

```text
FT.CREATE orders_idx ON JSON PREFIX 1 orders: SCHEMA $.status AS status TAG
```

The default `format: string` continues to use `MGET` and `SET`. Existing values
must use the binding's configured storage type. Changing `format` or
`keyEncoding` for an existing prefix requires a standard-update backfill,
which clears that prefix before replay. Alternatively, use a new prefix.
Delta bindings must use a new prefix for these changes, since their backfills
do not clear existing keys.

### Updates and deletes

By default, the connector reads existing documents so Estuary can apply the
collection's reduction rules before writing updated documents. Setting a
binding's `delta_updates` to `true` skips this read. Each resulting delta
replaces the value at its Redis key, so use delta updates when each write
contains the document you want applications to read.
Switching from delta updates to standard updates requires a backfill.

When `hardDelete` is `true`, a source deletion with `/_meta/op` equal to `d`
removes its Redis key. When `false`, the connector stores the deletion document
like any other update. This applies to both standard and delta updates.
Removing a binding or materialization does not remove its existing keys.

### Backfills and existing keys

For standard updates, increasing a binding's backfill counter clears all
existing keys matching `<keyPrefix>:*` before replaying collection history.
This also removes keys absent from the replayed history. The connector uses
`SCAN` and batched `UNLINK`, treating any glob characters in the prefix literally.

Applying a new binding also clears its prefix when the collection's read schema
uses reductions other than `lastWriteWins`, such as `sum`, `append`, or `merge`.
Schemas containing references, or unavailable schemas, are cleared conservatively.
Replayed reductions start without old values, so sums and appended arrays are
not counted twice, even when keys arrive out of order. Subsequent updates reduce
against the newly written values.

A new binding with backfill counter zero preserves existing keys when its read
schema has no references and uses only default or explicit `lastWriteWins`
reductions. These updates replace whole documents, so replay cannot merge old
values into the result. Keys absent from the input remain until deleted or expired.
Only schema keywords are inspected for reductions; a field named `reduce` or
data in defaults and examples does not trigger a clear.

Reserve the entire `<keyPrefix>:` namespace for a single binding. Ordinary
restarts and configuration changes without a backfill preserve existing keys
when the runtime supplies the previous specification or matching saved connector
state.

Estuary saves the connector state returned when a binding is applied and
supplies it the next time. This preserves keys even without the previous
specification when the saved state matches the materialization name, key prefix,
and backfill counter. The saved metadata also preserves the last applied
backfill of inactive bindings within the same materialization.

A fresh preview has no previous specification. With
`flowctl raw preview-next`, `--initial-state` supplies saved connector state
before bindings are applied, so matching state preserves keys. With
`flowctl preview`, `--initial-state` is supplied only after bindings are
applied, so it cannot prevent prefix clearing.

Previews without matching saved state supplied to Apply clear all keys in
standard-update prefixes with nonzero backfill counters or schemas that may
merge existing values, including schemas with references or unavailable schemas.
These bindings must replay their full history; incremental input cannot restore
keys absent from that input. Replacement-only bindings with backfill counter
zero and reference-free schemas can accept incremental input without clearing
untouched keys, as described above.

Delta-update bindings do not clear their prefixes when applied or backfilled.
Replayed deltas replace matching values, and keys absent from the replayed
history remain until deleted or expired.

### Expiration

Set `ttlSeconds` on a binding to expire each key that many seconds after its
latest write. Every write resets the expiration. A value of `0` disables
expiration and removes any existing TTL when the key is next written.
RedisJSON writes use `JSON.SET` followed by `EXPIRE`, or `PERSIST` when the TTL
is zero. Materializations with RedisJSON bindings wrap each write batch in
`MULTI`/`EXEC` so documents and their expiration changes are applied atomically.
An entire materialization transaction can span multiple batches.

Expired keys are not recreated until another document for that key arrives.
Standard updates treat an expired or evicted key as having no existing
document, so earlier values are unavailable for reductions. Account for this
when materializing partial updates or running totals.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing
the catalog specification file. See
[connectors](../../../concepts/connectors.md#using-connectors) to learn more
about using connectors.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
| -------- | ----- | ----------- | ---- | ---------------- |
| **`/address`** | Address | Redis server, or initial Sentinel endpoint when Sentinel is configured, in `host:port` form. | string | Required |
| `/username` | Username | Redis ACL username, if required. | string | Optional |
| `/password` | Password | Redis authentication password. Stored as a secret. | string | Optional |
| `/database` | Database | Non-negative Redis database number. | integer | `0` |
| `/tls` | TLS | Connect using TLS and verify the server certificate. | boolean | `false` |
| `/hardDelete` | Hard Delete | Delete keys when documents are deleted in the source. | boolean | `false` |
| `/advanced/ssl_server_ca` | SSL Server CA | PEM CA bundle replacing system CAs. Requires TLS. Stored as a secret. | string | Optional |
| `/advanced/ssl_client_cert` | SSL Client Certificate | PEM client certificate for mutual TLS. Requires its key and TLS. Stored as a secret. | string | Optional |
| `/advanced/ssl_client_key` | SSL Client Key | PEM client private key. Requires its certificate and TLS. Stored as a secret. | string | Optional |
| `/advanced/ssl_server_name` | SSL Server Name | Certificate hostname and SNI override for all connections. | string | Remote hostname |
| `/advanced/ssl_skip_verify` | Skip Certificate Verification | Disable certificate and hostname verification. Requires TLS. | boolean | `false` |
| `/sentinel/masterName` | Primary Name | Monitored primary name. Enables Sentinel discovery. | string | Required within `sentinel` |
| `/sentinel/addresses` | Additional Sentinel Addresses | Additional `host:port` seed endpoints. | array of strings | Optional |
| `/sentinel/username` | Sentinel Username | Sentinel ACL username, separate from Redis credentials. | string | Optional |
| `/sentinel/password` | Sentinel Password | Sentinel password. Stored as a secret. | string | Optional |
| `/networkTunnel/sshForwarding/sshEndpoint` | SSH Endpoint | Bastion endpoint in `ssh://user@hostname[:port]` form. | string | Required within `sshForwarding` |
| `/networkTunnel/sshForwarding/privateKey` | SSH Private Key | Private key for the bastion. Stored as a secret. | string | Required within `sshForwarding` |

#### Bindings

| Property | Title | Description | Type | Required/Default |
| -------- | ----- | ----------- | ---- | ---------------- |
| **`/keyPrefix`** | Key Prefix | Non-empty prefix followed by the encoded collection key. | string | Required |
| `/ttlSeconds` | TTL Seconds | Key lifetime after each write, in seconds. `0` disables expiration. Must be between `0` and `9223372036`. | integer | `0` |
| `/delta_updates` | Delta Updates | Write updates without loading existing documents. | boolean | `false` |
| `/format` | Document Format | `string` for JSON text in Redis strings; `json` for RedisJSON values. | string | `string` |
| `/keyEncoding` | Key Encoding | `legacy` joins components with colons; `json` uses a JSON array to prevent collisions. | string | `legacy` |

### Sample

```yaml
materializations:
  acmeCo/redis:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-redis:v1
        config:
          address: redis.example.com:6379
          tls: true
          database: 0
          hardDelete: true
    bindings:
      - resource:
          keyPrefix: orders
        source: acmeCo/orders
      - resource:
          keyPrefix: events
          ttlSeconds: 86400
        source: acmeCo/events
```

The `orders` binding retains its keys until they are deleted. The `events`
binding expires keys one day after their latest write. Add your endpoint's
authentication credentials when configuring the connector if required.

## Limitations

Each materialization must run as a single shard. Writes are pipelined, and a
transaction that updates multiple keys is not atomic across those keys.
