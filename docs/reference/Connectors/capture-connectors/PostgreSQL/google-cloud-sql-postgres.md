---
description: Capture PostgreSQL changes from Google Cloud SQL instances with Estuary’s CDC connector. Setup guide includes logical replication, WAL handling, replication slots, publications, watermarks tables, and backfills.
---

# Google Cloud SQL for PostgreSQL

This connector uses change data capture (CDC) to continuously capture updates in a PostgreSQL database into one or more Estuary collections.

This connector is a variant of the [PostgreSQL connector](./PostgreSQL.md).
Refer to that page for additional connector features, usage, and the full
configuration reference. Information specific to Google Cloud SQL and its setup is
presented below.

## Supported versions and platforms

This connector supports PostgreSQL versions 10.0 and later.

## Prerequisites

You'll need a PostgreSQL database setup with the following:

- [Logical replication enabled](https://www.postgresql.org/docs/current/runtime-config-wal.html) — `wal_level=logical`
- [User role](https://www.postgresql.org/docs/current/sql-createrole.html) with `REPLICATION` attribute
- A [replication slot](https://www.postgresql.org/docs/current/warm-standby.html#STREAMING-REPLICATION-SLOTS). This represents a “cursor” into the PostgreSQL write-ahead log from which change events can be read.
  - Optional; if none exist, one will be created by the connector.
  - If you wish to run multiple captures from the same database, each must have its own slot.
    You can create these slots yourself, or by specifying a name other than the default in the advanced [configuration](./PostgreSQL.md#configuration).
- A [publication](https://www.postgresql.org/docs/current/sql-createpublication.html). This represents the set of tables for which change events will be reported.
  - In more restricted setups, this must be created manually, but can be created automatically if the connector has suitable permissions.
- A watermarks table. The watermarks table is a small “scratch space” to which the connector occasionally writes a small amount of data to ensure accuracy when backfilling preexisting table contents.
  - In more restricted setups, this must be created manually, but can be created automatically if the connector has suitable permissions.
  - **For read-only environments**, the capture can operate in read-only mode which does not require a watermarks table. See [Read-Only Captures](./PostgreSQL.md#read-only-captures) for details.

## Setup

1. Allow connections between the database and Estuary. There are two ways to do this: by granting direct access to Estuary's IP or by creating an SSH tunnel.

   1. To allow direct access:

      - [Enable public IP on your database](https://cloud.google.com/sql/docs/mysql/configure-ip#add) and add the [Estuary IP addresses](/reference/allow-ip-addresses) as authorized IP addresses.

   2. To allow secure connections via SSH tunneling:
      - Follow the guide to [configure an SSH server for tunneling](../../../../../guides/connect-network/)
      - When you configure your connector as described in the [configuration](#configuration) section above, including the additional `networkTunnel` configuration to enable the SSH tunnel. See [Connecting to endpoints on secure networks](../../../../concepts/connectors.md#connecting-to-endpoints-on-secure-networks) for additional details and a sample.

2. On Google Cloud, navigate to your instance's Overview page. Click "Edit configuration". Scroll down to the Flags section. Click "ADD FLAG". Set [the `cloudsql.logical_decoding` flag to `on`](https://cloud.google.com/sql/docs/postgres/flags) to enable logical replication on your Cloud SQL PostgreSQL instance.

3. In your PostgreSQL client, connect to your instance and issue the following commands to create a new user for the capture with appropriate permissions,
   and set up the watermarks table and publication.

```sql
CREATE USER flow_capture WITH REPLICATION
IN ROLE cloudsqlsuperuser LOGIN PASSWORD 'secret';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO flow_capture;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO flow_capture;
CREATE TABLE IF NOT EXISTS public.flow_watermarks (slot TEXT PRIMARY KEY, watermark TEXT);
GRANT ALL PRIVILEGES ON TABLE public.flow_watermarks TO flow_capture;
CREATE PUBLICATION flow_publication;
ALTER PUBLICATION flow_publication SET (publish_via_partition_root = true);
ALTER PUBLICATION flow_publication ADD TABLE public.flow_watermarks, <other_tables>;
```

where `<other_tables>` lists all tables that will be captured from. The `publish_via_partition_root`
setting is recommended (because most users will want changes to a partitioned table to be captured
under the name of the root table) but is not required.

4. In the Cloud Console, note the instance's host under Public IP Address. Its port will always be `5432`.
   Together, you'll use the host:port as the `address` property when you configure the connector.

### Capturing from Read-Only Standbys

If you are capturing from a read-only standby on RDS, you will need to set
`hot_standby_feedback = on` so that the standby replica will keep the
upstream database informed about what catalog metadata needs to be retained. To enable hot
standby feedback on a Google CLoud SQL PostgreSQL instance:

1. Navigate to your replica instance in the Google Cloud Console
2. Click "Edit configuration"
3. In the Flags section, click "ADD A DATABASE FLAG"
4. Select `hot_standby_feedback` and set it to `on`
5. Click "Save" and wait for the instance to restart

You can verify whether the setting is enabled by running `SHOW hot_standby_feedback;`

See the main [PostgreSQL capture reference](./PostgreSQL.md#capturing-from-read-only-standbys)
for more information on capturing from read-only standby instances.

## Sample

A minimal capture definition will look like the following:

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-google-cloud-sql-postgres:v3
        config:
          address: host:port
          database: postgres
          user: flow_capture
          credentials:
            auth_type: UserPassword
            password: <secret>
    bindings:
      - resource:
          stream: ${TABLE_NAME}
          namespace: ${TABLE_NAMESPACE}
        target: ${PREFIX}/${COLLECTION_NAME}
```
