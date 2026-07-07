# Dremio

[Dremio](https://www.dremio.com) is a lakehouse platform with a built-in
Iceberg REST catalog based on Apache Polaris. Dremio Cloud's catalog exposes a
standard Iceberg REST endpoint, so you can materialize Estuary collections
into it as Iceberg tables and query them directly from Dremio.

This connector is a variant of the [Apache Iceberg
connector](./apache-iceberg.md). The setup steps are the same — refer to that
page for the full configuration reference, including EMR Serverless setup. The
only Dremio-specific configuration is the catalog connection below.

## Prerequisites

- A Dremio Cloud project with its catalog backed by Amazon S3. Only
  S3-backed catalogs are currently supported.
- A Dremio **Service User** for machine-to-machine authentication. Create one
  in the Dremio console under **Settings → User Management → Service Users**,
  and copy the **Client ID** and **Client Secret** when they are shown. Grant
  the service user privileges to create and write tables in the catalog
  namespace you intend to materialize into.
- An **AWS EMR Serverless application on release emr-7.11.0 or newer**
  (Iceberg 1.9.0+). Dremio vends short-lived S3 credentials with a relative
  refresh endpoint; only Iceberg 1.9.0 and later both resolve that endpoint
  against the catalog URL and use the vended credentials under OAuth client
  credentials auth. On older releases the Spark write job fails with either
  `Target host is not specified` (EMR ≤7.9, Iceberg ≤1.7) or a `401`
  authorization error (EMR 7.10, Iceberg 1.8). Validated on emr-7.12.0.

## Catalog Configuration

When configuring the materialization, use the following:

- **Base URL**: `https://catalog.dremio.cloud/api/iceberg`
- **Warehouse**: The name of your Dremio catalog. The default catalog in each
  Dremio Cloud project has the same name as the project.
- **Namespace**: Any namespace you desire. It will be created if it does not
  already exist.
- **Catalog Authentication**: Select **OAuth 2.0 Client Credentials**:
  - Set **OAuth 2.0 Server URI** to `https://login.dremio.cloud/oauth/token`.
    Dremio's token endpoint is on a different host than the catalog, so the
    full URL is required here.
  - Set **Catalog Credential** to `<client id>:<client secret>` using the
    service user credential you created above.
  - Set **Scope** to `dremio.all`.

:::tip
A Dremio [personal access
token](https://docs.dremio.com/dremio-cloud/security/authentication/personal-access-token/)
also works as the **Catalog Credential** (enter the bare token, with no
colon), but PATs expire after at most 180 days and the materialization will
fail when the token expires. Use a service user for production deployments.
:::

For all other configuration options (EMR Serverless compute, staging bucket,
IAM roles, bindings), refer to the [Apache Iceberg connector
docs](./apache-iceberg.md).
