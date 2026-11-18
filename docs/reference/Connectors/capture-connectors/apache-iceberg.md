---
description: Capture Apache Iceberg tables through a REST catalog, with an initial backfill and incremental reads of appended data files.
---

# Apache Iceberg

This connector captures tables from an [Apache Iceberg](https://iceberg.apache.org/) REST catalog into Estuary collections. Each table is discovered as a separate binding, with a recommended collection name of `<namespace>_<table>`.

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

The collection schema is generated from the Iceberg table schema. Optional fields allow `null`.

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
| `/credential` | Client Credential | Secret OAuth2 client credential in `id:secret` format. | string | Optional |
| `/token` | Bearer Token | Secret bearer token for the catalog. | string | Optional |
| `/scope` | Scope | OAuth2 authorization scope, such as `PRINCIPAL_ROLE:your_principal` for Polaris or Snowflake Open Catalog. | string | `catalog` |
| `/oauth2_server_uri` | OAuth 2.0 Server URI | Token endpoint URL or path relative to the catalog URL, such as `https://login.dremio.cloud/oauth/token`. | string | `v1/oauth/tokens` relative to the catalog URL |
| `/catalog_auth` | Catalog Authentication | AWS SigV4 or IAM authentication for the REST catalog. | object | `auth_type: None` |
| `/catalog_auth/auth_type` | Authentication Type | `None` to use the client credential or bearer token, `AWS SigV4` for supplied keys, or `AWSIAM` for an IAM role. | string | `None` |
| `/catalog_auth/aws_region` | AWS Region | Region of the catalog when using supplied keys. IAM uses `/credentials/aws_region`. | string | `us-east-1` for `AWS SigV4` |
| `/catalog_auth/signing_name` | Signing Name | AWS signing service: `glue` or `s3tables`. | string | `glue` |
| `/catalog_auth/aws_access_key_id` | AWS Access Key ID | Catalog access key. | string | Required for `AWS SigV4` |
| `/catalog_auth/aws_secret_access_key` | AWS Secret Access Key | Secret catalog access key. | string | Required for `AWS SigV4` |
| `/catalog_auth/aws_session_token` | AWS Session Token | Secret session token for supplied temporary credentials. | string | Optional for `AWS SigV4` |
| `/credentials` | Credentials | Shared IAM role used when `catalog_auth` or `s3` selects `AWSIAM`. | object | `auth_type: None` |
| `/credentials/auth_type` | Authentication Type | `None`, or `AWSIAM` to assume an IAM role. | string | `None`; `AWSIAM` required when `catalog_auth` or `s3` selects `AWSIAM` |
| `/credentials/aws_region` | AWS Region | AWS region used for IAM authentication and catalog signing. | string | Required for `AWSIAM` |
| `/credentials/aws_role_arn` | AWS Role ARN | Single IAM role that Estuary assumes for the catalog and/or S3. | string | Required for `AWSIAM` |
| `/namespaces` | Namespaces | Namespaces to discover. Use dots between nested namespace levels. When specified, only the listed namespaces are searched; otherwise discovery includes all namespaces and their children. | array of strings | All namespaces |
| `/s3` | S3 Configuration | Storage properties that override or augment those returned by the catalog. | object | `auth_type: None` |
| `/s3/auth_type` | Authentication Type | `None` for the catalog's storage settings, `AWSAccessKey` for supplied keys, or `AWSIAM` for an IAM role. | string | `None` |
| `/s3/endpoint` | S3 Endpoint | Endpoint URL for S3-compatible storage. | string | Catalog default |
| `/s3/region` | S3 Region | Storage region. | string | `us-east-1` |
| `/s3/access_key_id` | Access Key ID | Access key for reading storage. | string | Required for `AWSAccessKey` |
| `/s3/secret_access_key` | Secret Access Key | Secret key for reading storage. | string | Required for `AWSAccessKey` |
| `/s3/session_token` | Session Token | Secret session token for supplied temporary storage credentials. | string | Optional for `AWSAccessKey` |
| `/s3/path_style_access` | Path Style Access | Use path-style addressing for S3-compatible storage. | boolean | `false` |

Use the authentication method your REST catalog supports: an OAuth2 client credential, a bearer token, or `catalog_auth` for AWS. Omit all three for a catalog that does not require authentication. An `s3` object that sets keys but omits `auth_type` is treated as `AWSAccessKey`. The S3 `endpoint`, `region`, and `path_style_access` options apply to `AWSAccessKey` authentication.

### AWS catalogs and IAM roles

For Glue Data Catalog, set `catalog_uri` to `https://glue.<region>.amazonaws.com/iceberg`, `warehouse` to your AWS account ID, and `signing_name` to `glue`. For the S3 Tables REST endpoint, use `https://s3tables.<region>.amazonaws.com/iceberg`, the table bucket ARN as `warehouse`, and `s3tables` as `signing_name`. Use the catalog's AWS region for signing.

Follow the [AWS IAM guide](/guides/iam-auth/aws.md) to establish trust for Estuary. Set the role ARN and region in the top-level `credentials` object, then select `auth_type: AWSIAM` in `catalog_auth`, `s3`, or both. Flow supplies temporary session credentials under `credentials` at runtime, so you do not configure static keys. Both IAM selections use the same assumed role and region; separate catalog and storage IAM roles are not supported. When used for both, the role needs permission to list and load the catalog's tables and read their underlying storage. Glue catalogs governed by Lake Formation also require the appropriate Lake Formation grants.

Omit `s3` to use storage credentials supplied by the catalog, falling back to the AWS catalog credentials when no storage credentials are supplied. Catalog-supplied credentials are reloaded from the catalog every 30 minutes during long reads, and once more if storage reports an expired token. Configure `s3` to select separate supplied keys or the shared IAM role; explicit storage credentials override catalog credentials as a complete set, including the session token.

For example, Glue and S3 can share one role:

```yaml
catalog_uri: https://glue.us-east-1.amazonaws.com/iceberg
warehouse: "123456789012"
credentials:
  auth_type: AWSIAM
  aws_region: us-east-1
  aws_role_arn: arn:aws:iam::123456789012:role/iceberg-reader
catalog_auth:
  auth_type: AWSIAM
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

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-iceberg:v1
        config:
          catalog_uri: https://catalog.example.com
          warehouse: analytics
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
