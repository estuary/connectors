---
description: Materialize data into a Bauplan data lake. This variation of Estuary's Apache Iceberg connector uses the Bauplan REST catalog.
---

# Bauplan

[Bauplan](https://bauplanlabs.com/) is a serverless data lake platform built natively on Apache Iceberg. It provides a managed REST catalog so you can run SQL and Python queries directly on your Iceberg tables without managing catalog infrastructure yourself.

This connector materializes Estuary collections into Bauplan as Iceberg tables. The connector is a variant of the [Apache Iceberg connector](./apache-iceberg.md). The setup steps are the same — refer to that page for the full configuration reference, including EMR Serverless setup. The only Bauplan-specific configuration is the catalog connection below.

:::tip
For a complete end-to-end setup guide, see the Bauplan documentation: **[Estuary via EMR](https://docs.bauplanlabs.com/integrations/data_int_and_etl/estuary)**
:::

## Catalog Configuration

Bauplan exposes a standard Iceberg REST catalog endpoint. When configuring the materialization, use the following:

- **Base URL**: Your Bauplan REST catalog URL (available from your Bauplan account)
- **Warehouse**: Your Bauplan warehouse name
- **Catalog Authentication**: Select **OAuth 2.0 Client Credentials** and supply the client ID and secret from your Bauplan account

For all other configuration options (EMR Serverless compute, staging bucket, IAM roles, bindings), refer to the [Apache Iceberg connector docs](./apache-iceberg.md).

### Sample

```yaml
materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-bauplan:v1
        config:
          url: https://api.use1.aprod.bauplanlabs.com/iceberg
          warehouse: default
          namespace: <namespace>
          base_location: s3://<bucket>/iceberg
          credentials:
            auth_type: OAuth 2.0 Client Credentials
            oauth2_server_uri: v1/oauth/tokens
            credential: <client-id>:<client-secret>
          compute:
            region: us-east-1
            application_id: <emr-app-id>
            execution_role_arn: <emr-arn>
            bucket: <bucket>
            credentials:
              auth_type: AWSAccessKey
              aws_access_key_id: <aws-access-key-id>
              aws_secret_access_key: <aws-secret-access-key>
    bindings:
      - resource:
          table: ${COLLECTION_NAME}
        source: ${PREFIX}/${COLLECTION_NAME}
```
