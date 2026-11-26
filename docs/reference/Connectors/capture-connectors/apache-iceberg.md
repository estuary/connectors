---
description: Capture Apache Iceberg tables through a REST catalog, with an initial backfill and incremental reads of appended data files.
---

# Apache Iceberg

This connector captures tables from an [Apache Iceberg](https://iceberg.apache.org/) REST catalog into Estuary collections. Each table is discovered as a separate binding, with a recommended collection name of `<namespace>_<table>`.

Only the local Apache Iceberg REST catalog fixture has been tested end to end, using no catalog authentication and explicit access keys for a local S3-compatible store. Hosted catalogs and their authentication methods have not been tested against real services:

| Catalog or authentication method | Validation status |
|---|---|
| Local Apache REST catalog fixture with a local S3-compatible store | Tested |
| AWS Glue Data Catalog and S3 Tables | Untested against a real service |
| Polaris, Snowflake Open Catalog, and Dremio | Untested against a real service |
| OAuth2 client credentials and bearer tokens | Untested against a real service |
| AWS SigV4 access keys and assumed IAM roles | Untested against a real service |
| Catalog-supplied storage credentials and authenticated remote S3 signing | Untested against a real service |

:::warning
This capture is not change data capture. Rows deleted in Iceberg are never deleted downstream, regardless of the collection key.

After an overwrite or delete snapshot, the table is re-read:

- Without identifier fields, rows arrive again as new documents with new `_meta` keys. Earlier documents remain downstream.
- With identifier fields, re-read rows replace earlier documents with the same key, so updates reach standard materializations without creating duplicates for unchanged keys.

Replace snapshots, including file compaction and rewrites that preserve all rows, are skipped after the initial backfill. They do not re-emit documents. We recommend append-only tables when identifier fields are not set.
:::

## Prerequisites

- An Iceberg REST catalog reachable from Estuary, with permission to list namespaces and tables and load table metadata.
- Read access to the table metadata, manifests, data files, and supported delete files in object storage. The catalog can supply storage credentials, or you can configure S3 credentials explicitly.
- Snapshot retention long enough to cover the polling interval and any capture downtime. Unprocessed snapshots and their files must remain available until the capture reads them.

The connector reads Parquet, ORC, and Avro data files, including tables containing multiple formats. ORC field IDs and Avro field IDs preserve column identity across schema changes. Imported ORC and Avro files without field IDs require the table's Iceberg name mapping (`schema.name-mapping.default`). ORC timestamp values before 1677-09-21 or after 2262-04-11 cannot be read; the capture stops with an error naming the file. Position delete files in Parquet, ORC, and Avro formats are supported. Equality delete files are not supported. A top-level column named `_meta` conflicts with the connector's document metadata and cannot be captured.

Discovery skips unreadable tables and unsupported schemas with a warning. When `namespaces` is empty, discovery also warns and skips namespaces that disappear or deny access while listing their tables or child namespaces. Listing errors for explicitly configured namespaces fail discovery. Namespace components containing literal dots are unsupported and are also skipped; dots separate nested namespace levels.

## How data is captured

The first run reads the current snapshot in full, including when it is a replace snapshot. It streams data in batches of up to 1,000 rows from one file at a time and checkpoints progress after each file and every 100,000 rows within a file. Avro decoding buffers one compressed block at a time. The snapshot is marked complete after all of its files have been captured.

On subsequent polls, the connector follows the snapshot history after the last completed snapshot. For append snapshots, it reads only the added data files. For overwrite and delete snapshots, it logs the operation and reads the whole table as it existed at that snapshot. For replace snapshots, it logs the operation and advances the checkpoint without reading data files. A poll with no new snapshot emits no documents.

If a capture stops before completing a snapshot, it resumes at the first unfinished file. If required snapshot history has expired or the table has been rolled back past the saved snapshot, the capture fails with a request to re-backfill the binding. Re-backfilling reads the current table again.

### Document keys and metadata

Discovery uses the table's identifier fields as the collection key when they are set. Otherwise, it uses `/_meta/snapshot_id`, `/_meta/file`, and `/_meta/row` together. All documents include:

| Field | Description |
|---|---|
| `_meta.snapshot_id` | The integer ID of the snapshot being captured. |
| `_meta.file` | The data file path. |
| `_meta.row` | The zero-based physical row index within that file. |

### Data types

The collection schema is generated from the Iceberg table schema. Optional fields allow `null`. Field IDs preserve renamed fields when reading older files, including struct fields inside nested lists and maps.

| Iceberg type | JSON representation |
|---|---|
| Boolean, integer, long | Boolean or number. |
| Float, double | Number for finite values; strings `"NaN"`, `"Infinity"`, and `"-Infinity"` for non-finite values. |
| Decimal | Fixed-point string at the column's scale, for example `0.0000001000` for `decimal(38,10)`. |
| Timestamp, timestamp with time zone | RFC3339 string in UTC with a `Z` suffix; a zero fractional part is omitted, for example `2026-01-02T03:04:05Z`. Timestamps without a time zone are interpreted as UTC. Parquet INT96 timestamps are read at microsecond precision. In ORC files, values before 1677-09-21 or after 2262-04-11 are not supported. |
| Date, time | ISO 8601 string. |
| String, UUID | String. |
| Binary, fixed | Base64-encoded string. |
| Struct, list | Object or array, with values converted recursively. |
| Map with string keys | Object. |
| Map with other key types | Array of objects containing `key` and `value`. |

Timestamps and dates must fall in years 0000 through 9999. A value outside that range stops the capture with an error naming the column, data file, and row.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file. See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/catalog_uri`** | Catalog URI | URL of the Iceberg REST catalog. | string | Required |
| `/warehouse` | Warehouse | Warehouse name or location passed to the catalog. | string | Catalog default |
| `/credentials` | Credentials | Authentication method for the REST catalog. All options except `None` are untested against real services. | object | `auth_type: None` |
| `/credentials/auth_type` | Authentication Type | `None`, `OAuth2`, `Bearer Token`, `AWS SigV4`, or `AWSIAM`. | string | Required when `credentials` is provided |
| `/credentials/credential` | Client Credential | Secret OAuth2 client credential in `id:secret` format. | string | Required for `OAuth2` |
| `/credentials/token` | Bearer Token | Secret bearer token for the catalog. With `OAuth2`, an optional initial token, refreshed using the client credential when it expires. | string | Required for `Bearer Token`; optional for `OAuth2` |
| `/credentials/scope` | Scope | OAuth2 authorization scope, such as `PRINCIPAL_ROLE:your_principal`. | string | Empty; uses `catalog` |
| `/credentials/oauth2_server_uri` | OAuth 2.0 Server URI | Token endpoint URL or path relative to the catalog URL. | string | Empty; uses `v1/oauth/tokens` relative to the catalog URL |
| `/credentials/aws_region` | AWS Region | AWS region used for catalog signing. | string | `us-east-1` for `AWS SigV4`; required for `AWSIAM` |
| `/credentials/signing_name` | Signing Name | AWS signing service: `glue` or `s3tables` (both untested against real services). | string | `glue` |
| `/credentials/aws_access_key_id` | AWS Access Key ID | Catalog access key. | string | Required for `AWS SigV4` |
| `/credentials/aws_secret_access_key` | AWS Secret Access Key | Secret catalog access key. | string | Required for `AWS SigV4` |
| `/credentials/aws_session_token` | AWS Session Token | Secret session token for supplied temporary credentials. | string | Optional for `AWS SigV4` |
| `/credentials/aws_role_arn` | AWS Role ARN | IAM role that Estuary assumes for the catalog and optionally S3. | string | Required for `AWSIAM` |
| `/namespaces` | Namespaces | Namespaces to discover. Use dots between nested namespace levels. When specified, only the listed namespaces are searched; otherwise discovery includes all namespaces and their children. | array of strings | All namespaces |
| `/s3` | S3 Configuration | Storage properties that override or augment those returned by the catalog. | object | `auth_type: None` |
| `/s3/auth_type` | Authentication Type | `None` for the catalog's storage settings, `AWSAccessKey` for supplied keys, or `AWSIAM` to use the catalog's IAM role. | string | Required when `s3` is provided |
| `/s3/endpoint` | S3 Endpoint | Endpoint URL for S3-compatible storage. | string | Catalog default |
| `/s3/region` | S3 Region | Storage region. | string | `us-east-1` |
| `/s3/access_key_id` | Access Key ID | Access key for reading storage. | string | Required for `AWSAccessKey` |
| `/s3/secret_access_key` | Secret Access Key | Secret key for reading storage. | string | Required for `AWSAccessKey` |
| `/s3/session_token` | Session Token | Secret session token for supplied temporary storage credentials. | string | Optional for `AWSAccessKey` |
| `/s3/path_style_access` | Path Style Access | Use path-style addressing for S3-compatible storage. | boolean | `false` |

Choose the catalog's authentication method in `credentials`. Omit `credentials` for a catalog that does not require authentication. Both `credentials` and `s3` require an explicit `auth_type` when provided and cannot be `null`. The S3 `endpoint`, `region`, and `path_style_access` options apply to `AWSAccessKey` authentication.

Authenticated remote S3 signing is implemented using the catalog's authentication session, including bearer tokens obtained through OAuth2 client credentials. This path has not been tested against a real service.

### AWS catalogs and IAM roles (untested)

The following settings describe implemented configuration paths; neither AWS catalog nor IAM authentication has been tested against a real service. For Glue Data Catalog, set `catalog_uri` to `https://glue.<region>.amazonaws.com/iceberg`, `warehouse` to your AWS account ID, and `credentials.signing_name` to `glue`. For the S3 Tables REST endpoint, use `https://s3tables.<region>.amazonaws.com/iceberg`, the table bucket ARN as `warehouse`, and `s3tables` as `credentials.signing_name`. Use the catalog's AWS region for signing.

Follow the [AWS IAM guide](/guides/iam-auth/aws.md) to establish trust for Estuary. Set `auth_type: AWSIAM`, the role ARN, region, and signing name in `credentials`. Estuary supplies temporary session credentials under `credentials` at runtime, so you do not configure static keys. Select `auth_type: AWSIAM` in `s3` to use the same assumed role and region for storage. This requires IAM catalog credentials; separate catalog and storage IAM roles are not supported. When used for both, the role needs permission to list and load the catalog's tables and read their underlying storage. Glue catalogs governed by Lake Formation also require the appropriate Lake Formation grants; this setup is untested.

Omit `s3` to use storage credentials supplied by the catalog, falling back to the AWS catalog credentials when no storage credentials are supplied. Catalog-supplied credentials are reloaded from the catalog every 30 minutes during long reads, and once more if storage reports an expired token. Configure `s3` to select separate supplied keys or the shared IAM role; explicit storage credentials override catalog credentials as a complete set, including the session token.

Example configuration for Glue and S3 to share one role (untested):

```yaml
catalog_uri: https://glue.us-east-1.amazonaws.com/iceberg
warehouse: "123456789012"
credentials:
  auth_type: AWSIAM
  aws_region: us-east-1
  aws_role_arn: arn:aws:iam::123456789012:role/iceberg-reader
  signing_name: glue
s3:
  auth_type: AWSIAM
```

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/namespace`** | Namespace | Namespace containing the table, using dots for nested namespaces. | string | Required |
| **`/table`** | Table | Iceberg table name. | string | Required |
| `/interval` | Polling Interval | Positive ISO 8601 duration between checks for new snapshots. | string | `PT5M` |

### Sample

This example shows the configuration shape for OAuth2 client credentials. It has not been tested against a real OAuth2 service.

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-iceberg:v1
        config:
          catalog_uri: https://catalog.example.com
          warehouse: analytics
          credentials:
            auth_type: OAuth2
            credential: <client-id>:<client-secret>
          namespaces: [shop]
          s3:
            auth_type: AWSAccessKey
            region: us-east-1
            access_key_id: <access-key-id>
            secret_access_key: <secret-access-key>
    bindings:
      - resource:
          namespace: shop
          table: orders
          interval: PT5M
        target: ${PREFIX}/shop_orders
```
