---
description: Capture Supabase updates with Estuary's CDC connector. Setup guide includes logical replication, WAL handling, replication slots, publications, watermarks tables, and backfills.
---

# Supabase

This connector uses change data capture (CDC) to continuously capture updates in a Supabase PostgreSQL database into one or more Estuary collections.

This connector is a variant of the [PostgreSQL connector](./PostgreSQL.md).
Refer to that page for additional connector features, usage, and the full
configuration reference. Information specific to Supabase and its setup is
presented below.

## Supported versions and platforms

This connector supports all Supabase PostgreSQL instances.

## Prerequisites

You'll need a Supabase PostgreSQL database setup with the following:
* A Supabase IPv4 address and direct connection hostname which bypasses the Supabase connection pooler.
  See [Direct Database Connection](#direct-database-connection) for details.
* [Logical replication enabled](https://www.postgresql.org/docs/current/runtime-config-wal.html) — `wal_level=logical`
* [User role](https://www.postgresql.org/docs/current/sql-createrole.html) with `REPLICATION` attribute
* A [replication slot](https://www.postgresql.org/docs/current/warm-standby.html#STREAMING-REPLICATION-SLOTS). This represents a “cursor” into the PostgreSQL write-ahead log from which change events can be read.
    * Optional; if none exist, one will be created by the connector.
    * If you wish to run multiple captures from the same database, each must have its own slot.
    You can create these slots yourself, or by specifying a name other than the default in the advanced [configuration](#configuration).
* A [publication](https://www.postgresql.org/docs/current/sql-createpublication.html). This represents the set of tables for which change events will be reported.
    * In more restricted setups, this must be created manually, but can be created automatically if the connector has suitable permissions.
* A watermarks table. The watermarks table is a small “scratch space” to which the connector occasionally writes a small amount of data to ensure accuracy when backfilling preexisting table contents.
    * In more restricted setups, this must be created manually, but can be created automatically if the connector has suitable permissions.

:::tip Configuration Tip
To capture data from databases hosted on your internal network, you may need to
use [SSH tunneling](/guides/connect-network/). If you have a
[private deployment](/getting-started/deployment-options/#private-deployment),
you can also use private cloud networking features to reach your database.
:::

### Direct Database Connection

By default, Supabase guides users into connecting to their database through a
[Connection Pooler](https://supabase.com/docs/guides/database/connecting-to-postgres#connection-pooler).
Connection poolers are helpful for many applications, but unfortunately the pooler
does not support the CDC replication features that this connector relies on.

This capture connector requires a direct connection address for your database.
This address can be found by navigating to `Settings > Database` in the Supabase
dashboard and then making sure that the `Display connection pooler` checkbox is
**unchecked** so that the appropriate connection information is shown for a direct
connection.

You will also need to configure a [dedicated IPv4 address](https://supabase.com/docs/guides/platform/ipv4-address)
for your database, if you have not already done so. This can be configured under `Project Settings > Add Ons > Dedicated IPv4 address`
in the Supabase dashboard.

## Setup

The simplest way to meet the above prerequisites is to change the WAL level and have the connector use a database superuser role.

For a more restricted setup, create a new user with just the required permissions as detailed in the following steps:

1. Connect to your instance and create a new user and password:
```sql
CREATE USER flow_capture WITH PASSWORD 'secret' REPLICATION;
```
2. Assign the appropriate role.
    1. If using PostgreSQL v14 or later:

    ```sql
    GRANT pg_read_all_data TO flow_capture;
    ```

    2. If using an earlier version:

    ```sql
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES to flow_capture;
    GRANT SELECT ON ALL TABLES IN SCHEMA public, <other_schema> TO flow_capture;
    GRANT SELECT ON ALL TABLES IN SCHEMA information_schema, pg_catalog TO flow_capture;
    ```

    where `<other_schema>` lists all schemas that will be captured from.
    :::info
    If an even more restricted set of permissions is desired, you can also grant SELECT on
    just the specific table(s) which should be captured from. The ‘information_schema’ and
    ‘pg_catalog’ access is required for stream auto-discovery, but not for capturing already
    configured streams.
    :::
3. Create the watermarks table, grant privileges, and create publication:

```sql
CREATE TABLE IF NOT EXISTS public.flow_watermarks (slot TEXT PRIMARY KEY, watermark TEXT);
GRANT ALL PRIVILEGES ON TABLE public.flow_watermarks TO flow_capture;
CREATE PUBLICATION flow_publication;
ALTER PUBLICATION flow_publication SET (publish_via_partition_root = true);
ALTER PUBLICATION flow_publication ADD TABLE public.flow_watermarks, <other_tables>;
```

where `<other_tables>` lists all tables that will be captured from. The `publish_via_partition_root`
setting is recommended (because most users will want changes to a partitioned table to be captured
under the name of the root table) but is not required.

4. Set WAL level to logical:
```sql
ALTER SYSTEM SET wal_level = logical;
```
5. Restart PostgreSQL to allow the WAL level change to take effect.

### Sample

A minimal capture definition will look like the following:

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-supabase-postgres:v3
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
Your capture definition will likely be more complex, with additional bindings for each table in the source database.

[Learn more about capture definitions.](/concepts/captures.md)
