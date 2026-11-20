---
description: Capture Azure Cosmos DB for NoSQL containers into Estuary collections using the latest version change feed.
---

# Azure Cosmos DB for NoSQL

This connector captures containers from one or more [Azure Cosmos DB for NoSQL](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/) databases in an account into Estuary collections. For accounts using the MongoDB API, use the [Azure Cosmos DB for MongoDB connector](./MongoDB/azure-cosmosdb.md).

## Supported data resources

Each container is discovered as a separate binding. Choose exactly one database selection:

- `database`: capture one database. The recommended collection name and resource path remain the container name, preserving existing captures.
- `databases`: capture an explicit list of databases. Each binding includes its database, with a recommended name of `database/container` and resource path `[database, container]`.
- `all_databases: true`: list the account's databases and discover their containers, using the same qualified names and paths as `databases`. This requires permission to list and read databases across the account.

Containers with the same name in different databases have separate bindings and checkpoints. Re-discover to add new databases or containers to a capture. Switching from `database` to `databases` or `all_databases` changes resource paths; recreate the bindings with qualified resource configs and plan for a fresh capture at the configured start position.

The connector reads the [change feed in latest version mode](https://learn.microsoft.com/en-us/azure/cosmos-db/change-feed). By default, the first capture starts at the beginning of the feed to backfill existing items. You can instead start at `Now` or an RFC 3339 timestamp using `advanced.start_time`. After that, it resumes from saved continuation tokens that track progress for each feed range, including across capture restarts. Unchanged items are not captured again.

The feed provides the latest version of an item. If an item is updated several times between polls, only its latest version is captured. This connector does not provide a history of every intermediate update.

Dropping and recreating a container under the same name invalidates its saved continuation tokens. When the connector detects this, it restarts from the configured start position (`Beginning` by default). With `Now`, it instead restarts the recreated container from `Beginning` so that items written before detection are captured. Items from the previous container are not deleted from Estuary or your destination.

:::warning
Latest version mode does not report deletes, including items removed by time to live (TTL). Deleting an item in Cosmos DB does not remove it from Estuary or your destination. If you need to track deletions, store a deletion flag on the item and use that flag downstream.
:::

### Collection keys and documents

Collection keys combine the container's partition key paths, a partition key state field, and `/id`:

| Container partition key | Collection key |
|---|---|
| `/id` | `["/id"]` |
| `/customer` | `["/customer", "/_meta/partitionKeyState", "/id"]` |
| Hierarchical: `/tenant`, `/customer` | `["/tenant", "/customer", "/_meta/partitionKeyState", "/id"]` |

Nested partition key paths are preserved, and `/id` is included only once. Cosmos paths are converted to JSON Pointers for collection keys: for example, `/"tenant/id"` becomes `/tenant~1id`, while a literal `/tenant~1id` becomes `/tenant~01id`. Keep the discovered key so that items with the same ID in different partitions remain distinct.

Partition key fields in the collection are never `null`, so these collections can be materialized. Cosmos DB accepts items without a partition key value, or with an explicit `null` value. For these items, the connector writes a default at the partition key path: `""` for a string key, `0` for an integer key, or `false` for a boolean key. The default matches the key's type in the bound collection's schema; if that type can't be determined, the capture fails with an error naming the path. For a nested path, a parent that is missing, `null`, or not an object is replaced with an object holding the default, so a non-object parent value is not captured.

For integer partition keys within the signed or unsigned 64-bit range, the connector captures whole-number floats such as `42.0` as integers (`42`). This keeps the collection key stable when writers use different JSON number representations for the same item.

So that a default does not collide with a real value, each document also carries `_meta/partitionKeyState`, which is part of the collection key. For each partition key path, in order, it holds `value` when the item has a non-null value, `null` when the value is an explicit `null`, or `undefined` when the property is missing or a parent on the path is missing, `null`, or not an object. Values for multiple paths are joined by commas, for example `value,undefined`. An item with `{"customer": ""}` and an item with no `customer` both have `""` at `/customer`, but their states (`value` and `undefined`) keep them apart. Containers keyed only on `/id` have no state field, because every item has an ID.

Discovery samples one non-null value for each partition key path to choose a string, integer, or boolean schema. Containers with no non-null value default to a string schema. The `/id` field is always a string. Flow requires each key field to have a single scalar type, so mixed scalar types in one partition key path are not supported. If an item later has a partition key value of another type, for example a number in a container that was empty when discovered, the capture fails with an error naming the item and path. If the container's items now share one string, integer, or boolean type, re-discover and backfill the binding to change the key type. Re-discovery does not help containers that mix key types or hold non-integer numeric, object, or array partition key values; disable those bindings. Containers with a sampled non-integer numeric partition key, or whose sample query Cosmos DB rejects, are skipped with a warning naming the database, container, and path. Other containers remain available, including when using `all_databases`. Only discovery samples partition keys, so an existing binding is not skipped when the capture starts.

Legacy non-partitioned containers migrated by Cosmos DB use a system partition key (`systemKey`). These containers use `["/_partitionKey", "/_meta/partitionKeyState", "/id"]` as their collection key. Legacy items that lack `_partitionKey` get the default value and the state `undefined`; newer items keep their values with the state `value`. Like other containers, these collections can be materialized.

Containers with a partition key equal to or beneath `/_meta` are skipped with a warning because `_meta` is reserved for Estuary metadata. Other containers in the database can still be captured.

Captured documents retain the item's fields, including the Cosmos DB system properties `_rid`, `_ts`, and `_etag`. The connector drops `_self` and `_attachments` and adds Estuary's `_meta` metadata. A top-level `_meta` property on a Cosmos DB item is replaced by Estuary's metadata and is not captured.

## Prerequisites

- An Azure Cosmos DB account using the NoSQL API and an existing database.
- The account endpoint URI and either an account key or a Microsoft Entra identity. A read-only account key is sufficient; find these in the Azure portal under your account's **Keys** page.
- Network access from Estuary to your account. If the account uses an IP firewall, [allowlist Estuary's IP addresses](/reference/allow-ip-addresses).

When using the emulator or a private endpoint that requires requests to stay on the configured endpoint, set `advanced.disable_endpoint_discovery` to `true`. This setting does not establish private network connectivity; the configured endpoint must already be reachable from Estuary.

### Microsoft Entra authentication

For accounts with local authentication disabled (`disableLocalAuth`), set `credentials_title` to `Service Principal`. Supply your `tenant_id`, `client_id`, and the secret value in `client_secret`. Credentials and their token refresh are managed by the Azure Identity SDK. Managed Identity and Workload Identity authentication are not supported.

Assign the identity the **Cosmos DB Built-in Data Reader** role through [Cosmos DB data-plane RBAC](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/how-to-connect-role-based-access-control). This is distinct from Azure management-plane Reader or Cosmos DB Account Reader roles. The connector reads metadata, queries partition key samples, and reads the change feed; it does not write to Cosmos DB. Scope the role to each selected database, or to `/` for account-wide discovery with `all_databases`. Allow for role assignment propagation before validating the capture.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file. See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/endpoint`** | Endpoint | Account URI, such as `https://myaccount.documents.azure.com:443/`. | string | Required |
| **`/credentials`** | Authentication | An account key or a Microsoft Entra service principal. | object | Required |
| **`/credentials/credentials_title`** | Authentication Method | `Account Key` or `Service Principal`. | string | Required |
| `/credentials/key` | Account Key | Secret account key. A read-only key is sufficient. | string | Required for Account Key |
| `/credentials/tenant_id` | Tenant ID | Microsoft Entra tenant ID. | string | Required for Service Principal |
| `/credentials/client_id` | Client ID | Application client ID. | string | Required for Service Principal |
| `/credentials/client_secret` | Client Secret | Secret value for the service principal. | string | Required for Service Principal |
| `/database` | Database | One database to capture. Choose exactly one of `database`, `databases`, or `all_databases`. | string | One selection required |
| `/databases` | Databases | Nonempty list of distinct database names. | array of strings | One selection required |
| `/all_databases` | All Databases | Discover containers across all databases in the account. | boolean | `false` |
| `/advanced/disable_endpoint_discovery` | Disable Endpoint Discovery | Connect only to the configured endpoint, for example for the emulator or a private endpoint. | boolean | `false` |
| `/advanced/priority` | Request Priority | `Low` or `High` for change feed reads and partition key sampling queries. Requires priority-based execution enabled on the account. | string | Unset (service default) |
| `/advanced/throughput_bucket` | Throughput Bucket | Bucket ID from 1 through 5 for connector requests. Configure the bucket's throughput allocation on the account first. | integer | Unset |
| `/advanced/max_item_count` | Page Size Hint | Positive maximum number of items requested per change feed page. | integer | Unset (SDK default) |
| `/advanced/start_time` | Start Position | `Beginning`, `Now`, or an RFC 3339 timestamp with a timezone, such as `2026-11-11T00:00:00Z`. Applies when there is no saved position. | string | `Beginning` |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/container`** | Container | Name of the container to capture. | string | Required |
| `/database` | Database | Container's database. Omit when using the endpoint's single `database` setting. | string | Required for `databases` or `all_databases` |
| `/interval` | Polling Interval | Interval between change feed polls, as a positive ISO 8601 duration. For example, `PT5S` is five seconds and `PT1M` is one minute. | string | `PT1M` |

Each container is polled independently. Shorter intervals reduce capture latency and increase the number of requests to Cosmos DB, which consume request units (RUs).

The polling interval applies after a poll finds no changes. While catching up with changes, the connector polls again immediately after checkpointing.

### RU controls

Set `advanced.priority: Low` to have Cosmos DB throttle connector reads before high-priority requests when provisioned throughput is exhausted. Enable priority-based execution on the account first. `advanced.throughput_bucket` assigns requests to a bucket whose throughput allocation you configure in Cosmos DB; the connector does not create or size that bucket. Account and service support for these features is required.

`advanced.max_item_count` controls the requested change feed page size. Cosmos DB can return fewer items because of response size or other service limits. Smaller pages can reduce individual request costs but increase the number of requests. These settings do not impose a fixed RU-per-second limit on the capture. All settings are optional; omitting them retains the existing SDK and service defaults.

### Start position

`Beginning` backfills existing items. `Now` starts at the time each binding first polls. The connector saves that time before reading the feed, so an empty initial poll or a restart does not move the starting point forward. It also saves continuation progress from empty responses so initialization can advance across physical feed ranges. A timestamp requests changes from that point in time using the SDK's start-time option. The SDK sends start times in UTC at second precision; subsecond precision is not available.

Saved positions always take precedence over `advanced.start_time`. Changing the setting does not rewind or skip ahead in an existing binding. To apply a new start position, request a backfill/reset of the binding. Starting at `Now` or a timestamp omits older items that have not changed since that position. It still uses latest version mode and does not capture deletes or every intermediate version.

### Sample

```yaml
captures:
  acmeCo/cosmosdb:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-cosmosdb-nosql:v1
        config:
          endpoint: https://myaccount.documents.azure.com:443/
          credentials:
            credentials_title: Account Key
            key: <secret account key>
          database: shop
    bindings:
      - resource:
          container: orders
          interval: PT1M
        target: acmeCo/orders
      - resource:
          container: events
          interval: PT1M
        target: acmeCo/events
```

For a capture using a service principal and two databases, replace the endpoint config and use qualified binding configs:

```yaml
config:
  endpoint: https://myaccount.documents.azure.com:443/
  credentials:
    credentials_title: Service Principal
    tenant_id: <tenant ID>
    client_id: <application client ID>
    client_secret: <secret value>
  databases: [shop, billing]
  advanced:
    priority: Low
    max_item_count: 100
    start_time: Now
bindings:
  - resource:
      database: shop
      container: orders
    target: acmeCo/shop/orders
  - resource:
      database: billing
      container: orders
    target: acmeCo/billing/orders
```

Replace `databases: [shop, billing]` with `all_databases: true` for account-wide discovery.
