---
description: Materialize Estuary collections into Azure Cosmos DB for NoSQL containers with standard updates, delta updates, and hard deletes.
---

# Azure Cosmos DB for NoSQL

This connector materializes Estuary collections into Azure Cosmos DB for NoSQL
containers, storing one item per collection key. It uses the NoSQL API. For
Cosmos DB accounts using the MongoDB API, use the [MongoDB connector](./mongodb.md).

## Prerequisites

- An Azure Cosmos DB account using the NoSQL API.
- The account endpoint and a read-write account key from the account's **Keys**
  page in the Azure portal. Key-based authentication must be enabled.
- Network access from Estuary to the account. If the account restricts incoming
  connections, [allowlist Estuary's IP addresses](/reference/allow-ip-addresses).
- For bindings with standard updates, a default consistency level of Session,
  Bounded Staleness, or Strong on the account. Validation fails on Eventual or
  Consistent Prefix accounts unless every binding uses delta updates.

The connector creates the configured database and containers if they do not
exist. It does not specify throughput when creating them. Configure any required
database throughput in Azure before starting the materialization, or use an
account that supports creation without provisioned throughput, such as a
serverless account.

## Data model

Each item contains the full collection document, except for the reserved
fields listed below. The connector selects the root document and collection key
fields; other fields cannot be selected separately. They remain part of the full
document.

Cosmos DB requires a string `id`. The connector derives it from the collection's
key values, in key order, joined with `:`. Integers and booleans use their JSON
representation. String values keep their text, except that each byte of these
characters is written as `~` followed by two uppercase hex digits:

| Character | Encoding |
| --------- | -------- |
| `~` | `~7E` |
| `%` | `~25` |
| `:` | `~3A` |
| `/` | `~2F` |
| `\` | `~5C` |
| `?` | `~3F` |
| `#` | `~23` |
| Control characters | for example `~0A` for a newline |
| Trailing whitespace | for example `~20` for a space |
| A value of exactly `.` or `..` | `~2E` or `~2E~2E` |
| A single key that is the empty string | `~` |

For example, key `[1]` becomes `"1"`, and key `["eu/west", 8]` becomes
`"eu~2Fwest:8"`. If the document already has a top-level `id`, the connector
moves its original value to `_flow_id`. Reserve `_flow_id` for this purpose;
an existing field with that name would be overwritten when `id` is moved.
Top-level `_rid`, `_self`, `_etag`, `_attachments`, `_ts` and `_lsn` are also
reserved. Cosmos DB sets the first five on every write, and the connector
removes all six when it reads items back, so collection values in these fields
are not kept.
The connector preserves JSON numbers without converting them to floating point.

### Partition keys

Each container has one partition key path, chosen from the collection's key
JSON pointers. It defaults to the first collection key pointer. Set the binding's
`partition_key` to choose another one, such as `/seq` for a collection keyed by
`[/region, /seq]`.

The item's partition value is the value of that field. When the partition key
path is `/id`, the value is the encoded string `id`, including all collection
key components. Paths beneath `/id`, such as `/id/tenant`, cannot be used because
storage replaces `id` with that string. Other partition key fields must be
present in every document: a schema default fills the extracted key tuple but
does not add a missing field to the stored document. Choose a required key field
instead.

The connector translates JSON pointer escapes into Cosmos DB partition paths.
For example, `/a~1b` selects the property `a/b` and creates the Cosmos path
`/"a/b"`. Nested paths are supported. Empty property names and names containing
double quotes, backslashes, or control characters are not supported. An existing
container must have the corresponding Cosmos partition path; a mismatch fails
validation.

### Updates and deletes

With standard updates, the connector reads the existing item by its `id` and
partition value before Estuary applies the collection's reduction strategy. This
allows a partial update to preserve fields from the previous document. On read,
the connector removes Cosmos DB's system properties and restores `_flow_id` to
the document's original `id`.

Session tokens are saved in connector checkpoints per binding and physical
partition and restored on restart, so with the required account consistency,
reads observe acknowledged writes. Saved tokens are discarded if the container
was deleted and recreated. A session-unavailable response (404 with substatus
1002) fails the transaction rather than treating an existing item as absent.
Ordinary item-not-found responses still indicate an absent item.

With `delta_updates: true`, the connector skips these reads and upserts each
incoming document. The last document written for a key replaces the item, so
fields omitted from that document are removed. Delta updates do not append a
history of documents.

By default, source deletions are stored as documents with deletion metadata.
Set `hardDelete: true` to remove items when Estuary marks a document as deleted
or its `/_meta/op` is `d`. Hard deletes work in both update modes, including when
an item has not been loaded. Deleting an already absent item succeeds.

Reads, upserts, and deletes retry throttling responses (429) with up to 20
attempts. Retries honor Cosmos DB's `x-ms-retry-after-ms` delay and task
cancellation. The maximum retry delay is 60 seconds; exhausted retries fail the
transaction.

### Backfills

Requesting a new backfill for a binding drops and recreates its container before
materializing the collection again. This removes the container's existing data
and any custom container settings. The recreated container uses the binding's
partition key path and Azure's default settings. To change a binding's
partition key path, or after a collection reset changes it, backfill the
binding so its container is recreated with the new path.

## Configuration

Configure the connector in the Estuary web app or in a catalog specification.
See [using connectors](../../../concepts/connectors.md#using-connectors) for more
information.

### Endpoint

| Property | Title | Description | Type | Required/Default |
| -------- | ----- | ----------- | ---- | ---------------- |
| **`/endpoint`** | Endpoint | Account URI, such as `https://myaccount.documents.azure.com:443/`. HTTP is also supported for emulators. | string | Required |
| **`/key`** | Account Key | Read-write account key. This value is a secret. | string | Required |
| **`/database`** | Database | Destination database, created if missing. | string | Required |
| `/hardDelete` | Hard Delete | Remove source deletions from the destination. | boolean | `false` |
| `/advanced/feature_flags` | Feature Flags | Internal options; change only as directed by Estuary support. | string | Unset |

### Bindings

| Property | Title | Description | Type | Required/Default |
| -------- | ----- | ----------- | ---- | ---------------- |
| **`/container`** | Container Name | Destination container, created if missing. | string | Required |
| `/partition_key` | Partition Key | One of the collection's key JSON pointers. | string | First collection key pointer |
| `/delta_updates` | Delta Updates | Skip loading existing items and replace each item with the latest document for its key. | boolean | `false` |

### Sample

For a collection `acmeCo/events` keyed by `[/region, /seq]`:

```yaml
materializations:
  acmeCo/cosmos:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-cosmosdb-nosql:v1
        config:
          endpoint: https://myaccount.documents.azure.com:443/
          key: <read-write-account-key>
          database: analytics
          hardDelete: true
    bindings:
      - resource:
          container: events
          partition_key: /seq
        source: acmeCo/events
```

## Limitations

Version 1 does not support Microsoft Entra ID or resource-token authentication,
configuring throughput, indexing policies, or TTL on created containers,
hierarchical partition keys, or patch updates. Updates replace whole items.
The materialization must run with a single shard. The connector does not create
a checkpoint table.
