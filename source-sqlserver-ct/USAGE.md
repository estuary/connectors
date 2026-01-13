---
sidebar_position: 3
---

# Microsoft SQL Server (Change Tracking)

This connector uses SQL Server Change Tracking to continuously capture updates in a Microsoft SQL Server database into one or more Flow collections.

It's available for use in the Flow web application. For local development or open-source workflows, [`ghcr.io/estuary/source-sqlserver-ct:dev`](https://ghcr.io/estuary/source-sqlserver-ct:dev) provides the latest version of the connector as a Docker image. You can also follow the link in your browser to see past image versions.

## When to use this connector

Estuary offers two SQL Server connectors: this one (Change Tracking) and the
[CDC connector](http://go.estuary.dev/source-sqlserver). Both provide real-time
change capture with similar performance characteristics, but have different
strengths.

**Choose Change Tracking when:**

- You need to capture computed columns or computed primary keys (CDC cannot capture these)
- You want lower storage overhead (CT stores only primary keys, not full row contents)

**Choose CDC when:**

- You need to capture from tables without a primary key
- You need complete audit logging with full row history (CT may combine intermediate
  changes when they occur in rapid succession)

## Supported versions and platforms

This connector will work on both hosted deployments and all major cloud providers. It is designed for databases using any version of SQL Server which has Change Tracking support, and is regularly tested against SQL Server 2017 and up.

Setup instructions are provided for the following platforms:

- [Self-hosted SQL Server](#self-hosted-sql-server)
- [Azure SQL Database](#azure-sql-database)
- [Amazon RDS for SQL Server](#amazon-rds-for-sql-server)
- [Google Cloud SQL for SQL Server](#google-cloud-sql-for-sql-server)

## Prerequisites

To capture change events from SQL Server tables using this connector, you need:

- **Primary keys are required.** Each table to be captured must have a primary key defined. Change Tracking does not support tables without primary keys.

- [Change Tracking enabled](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/enable-and-disable-change-tracking-sql-server) on both the database and the individual tables to be captured.

- A user role with:
  - The `VIEW DATABASE STATE` permission (or `VIEW DATABASE PERFORMANCE STATE` in newer versions).
  - `SELECT` permissions on the schemas that contain tables to be captured.
  - `VIEW CHANGE TRACKING` permission on the schemas containing tables to capture.

## Setup

To meet these requirements, follow the steps for your hosting type.

### Self-hosted SQL Server

1. Connect to the server and issue the following commands:

```sql
USE <database>;
-- Enable Change Tracking for the database with a 2-day retention period.
ALTER DATABASE <database> SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);
-- Create user and password for use with the connector.
CREATE LOGIN flow_capture WITH PASSWORD = 'secret';
CREATE USER flow_capture FOR LOGIN flow_capture;
-- Grant the user permissions on schemas with data.
-- This assumes all tables to be captured are in the default schema, `dbo`.
-- Add similar queries for any other schemas that contain tables you want to capture.
GRANT SELECT ON SCHEMA :: dbo TO flow_capture;
GRANT VIEW CHANGE TRACKING ON SCHEMA :: dbo TO flow_capture;
-- Grant the 'VIEW DATABASE STATE' permission.
GRANT VIEW DATABASE STATE TO flow_capture;
-- Enable Change Tracking on tables. The below query enables CT on table 'dbo.foobar',
-- you should add similar query for all other tables you intend to capture.
ALTER TABLE dbo.foobar ENABLE CHANGE_TRACKING;
```

2. Allow secure connection to Estuary Flow from your hosting environment. Either:

   - Set up an [SSH server for tunneling](/guides/connect-network/).

     When you fill out the [endpoint configuration](#endpoint),
     include the additional `networkTunnel` configuration to enable the SSH tunnel.
     See [Connecting to endpoints on secure networks](/concepts/connectors.md#connecting-to-endpoints-on-secure-networks)
     for additional details and a sample.

   - [Allowlist the Estuary IP addresses](/reference/allow-ip-addresses) in your firewall rules.

### Azure SQL Database

1. Allow connections between the database and Estuary Flow. There are two ways to do this: by granting direct access to Flow's IP or by creating an SSH tunnel.

   1. To allow direct access:

      - Create a new [firewall rule](https://learn.microsoft.com/en-us/azure/azure-sql/database/firewall-configure?view=azuresql#use-the-azure-portal-to-manage-server-level-ip-firewall-rules) that grants access to the [Estuary Flow IP addresses](/reference/allow-ip-addresses).

   2. To allow secure connections via SSH tunneling:
      - Follow the guide to [configure an SSH server for tunneling](/guides/connect-network/)
      - When you configure your connector as described in the [configuration](#configuration) section above, including the additional `networkTunnel` configuration to enable the SSH tunnel. See [Connecting to endpoints on secure networks](/concepts/connectors.md#connecting-to-endpoints-on-secure-networks) for additional details and a sample.

2. In your SQL client, connect to your instance as the default `sqlserver` user and issue the following commands.

```sql
USE <database>;
-- Enable Change Tracking for the database.
ALTER DATABASE <database> SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);
-- Create user and password for use with the connector.
CREATE LOGIN flow_capture WITH PASSWORD = 'secret';
CREATE USER flow_capture FOR LOGIN flow_capture;
-- Grant the user permissions on schemas with data.
-- This assumes all tables to be captured are in the default schema, `dbo`.
-- Add similar queries for any other schemas that contain tables you want to capture.
GRANT SELECT ON SCHEMA :: dbo TO flow_capture;
GRANT VIEW CHANGE TRACKING ON SCHEMA :: dbo TO flow_capture;
-- Grant the 'VIEW DATABASE STATE' permission.
GRANT VIEW DATABASE STATE TO flow_capture;
-- Enable Change Tracking on tables. The below query enables CT on table 'dbo.foobar',
-- you should add similar query for all other tables you intend to capture.
ALTER TABLE dbo.foobar ENABLE CHANGE_TRACKING;
```

3. Note the following important items for configuration:

   - Find the instance's host under Server Name. The port is always `1433`. Together, you'll use the host:port as the `address` property when you configure the connector.

### Amazon RDS for SQL Server

1. Enable Change Tracking on the database:

```sql
USE <database>;
ALTER DATABASE <database> SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);
```

2. Create user and grant permissions:

```sql
CREATE LOGIN flow_capture WITH PASSWORD = 'secret';
CREATE USER flow_capture FOR LOGIN flow_capture;
GRANT SELECT ON SCHEMA :: dbo TO flow_capture;
GRANT VIEW CHANGE TRACKING ON SCHEMA :: dbo TO flow_capture;
GRANT VIEW DATABASE STATE TO flow_capture;
```

3. Enable Change Tracking on each table you want to capture:

```sql
ALTER TABLE dbo.foobar ENABLE CHANGE_TRACKING;
```

4. Configure network access to allow connections from Estuary Flow.

### Google Cloud SQL for SQL Server

1. Enable Change Tracking on the database:

```sql
USE <database>;
ALTER DATABASE <database> SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);
```

2. Create user and grant permissions:

```sql
CREATE LOGIN flow_capture WITH PASSWORD = 'secret';
CREATE USER flow_capture FOR LOGIN flow_capture;
GRANT SELECT ON SCHEMA :: dbo TO flow_capture;
GRANT VIEW CHANGE TRACKING ON SCHEMA :: dbo TO flow_capture;
GRANT VIEW DATABASE STATE TO flow_capture;
```

3. Enable Change Tracking on each table you want to capture:

```sql
ALTER TABLE dbo.foobar ENABLE CHANGE_TRACKING;
```

4. Configure network access to allow connections from Estuary Flow.

## Change Tracking Retention

Change Tracking data is automatically cleaned up based on the retention period configured when enabling Change Tracking (default is 2 days). If the connector is offline for longer than this retention period, or if it falls too far behind, you may need to perform a full re-backfill of affected tables.

To adjust the retention period:

```sql
ALTER DATABASE <database> SET CHANGE_TRACKING (CHANGE_RETENTION = 5 DAYS);
```

## Handling DDL Alterations to Source Tables

Change Tracking automatically handles schema changes to source tables. When columns are added or removed, the connector will detect these changes during the next discovery phase.

## Configuration

You configure connectors either in the Flow web app, or by directly editing the catalog specification file.
See [connectors](/concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the SQL Server Change Tracking source connector.

### Properties

#### Endpoint

| Property                        | Title               | Description                                                                                                                                 | Type    | Required/Default           |
| ------------------------------- | ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- | ------- | -------------------------- |
| **`/address`**                  | Server Address      | The host or host:port at which the database can be reached.                                                                                 | string  | Required                   |
| **`/database`**                 | Database            | Logical database name to capture from.                                                                                                      | string  | Required                   |
| **`/user`**                     | User                | The database user to authenticate as.                                                                                                       | string  | Required, `"flow_capture"` |
| **`/password`**                 | Password            | Password for the specified database user.                                                                                                   | string  | Required                   |
| `/historyMode` | History Mode | Capture each change event, without merging. | boolean | `false` |
| `/advanced`                     | Advanced Options    | Options for advanced users. You should not typically need to modify these.                                                                  | object  |                            |
| `/advanced/discover_tables_without_ct` | Discover Tables Without Change Tracking | When set, the connector will discover all tables even if they do not have Change Tracking enabled. By default, only CT-enabled tables are discovered. | boolean | `false` |
| `/advanced/backfill_chunk_size` | Backfill Chunk Size | The number of rows which should be fetched from the database in a single backfill query.                                                    | integer | `50000`                     |
| `/advanced/skip_backfills`      | Skip Backfills      | A comma-separated list of fully-qualified table names which should not be backfilled.                                                       | string  |                            |
| `/advanced/source_tag` | Source Tag | This value is added as the property 'tag' in the source metadata of each document. | string |  |

#### Bindings

| Property         | Title               | Description                                                                                                                                                                  | Type                                                          | Required/Default |
| ---------------- | ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------- | ---------------- |
| **`/namespace`** | Namespace           | The [namespace/schema](https://learn.microsoft.com/en-us/sql/relational-databases/databases/databases?view=sql-server-ver16#basic-information-about-databases) of the table. | string                                                        | Required         |
| **`/stream`**    | Stream              | Table name.                                                                                                                                                                  | string                                                        | Required         |
| `/primary_key`   | Primary Key Columns | array                                                                                                                                                                        | The columns which together form the primary key of the table. |                  |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: "ghcr.io/estuary/source-sqlserver-ct:dev"
        config:
          address: "<host>:1433"
          database: "my_db"
          user: "flow_capture"
          password: "secret"
    bindings:
      - resource:
          stream: ${TABLE_NAME}
          namespace: dbo
          primary_key: ["id"]
        target: ${PREFIX}/${COLLECTION_NAME}
```

Your capture definition will likely be more complex, with additional bindings for each table in the source database.

[Learn more about capture definitions.](/concepts/captures.md)

## Primary Key Requirement

Change Tracking requires all captured tables to have a primary key. Tables without primary keys cannot be captured using this connector.

If your table does not have a primary key defined in the database, you must either:
1. Add a primary key to the table in the database, or
2. Use the CDC-based SQL Server connector instead, which can work with tables that have unique indexes.

## Computed Columns

Computed columns and computed primary keys are fully supported.
