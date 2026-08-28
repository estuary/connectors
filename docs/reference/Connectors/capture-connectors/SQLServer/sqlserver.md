---
sidebar_position: 3
description: Set up Estuary's Microsoft SQL Server CDC connector with automatic capture instance management and change table cleanup using self-hosted and cloud platform guides.
---

# Microsoft SQL Server

This connector uses change data capture (CDC) to continuously capture updates in a Microsoft SQL Server database into one or more Estuary collections.

## When to use this connector

Estuary offers three main SQL Server capture connectors and their variants (platform-specific versions for managed providers). All three work across self-hosted and cloud-managed deployments.

| Connector | Mechanism | Latency | Read Replica Support | Key Strengths |
|-----------|-----------|---------|----------------------|---------------|
| **CDC** (this connector) | Log-based change capture | Real-time | Yes\* | Full audit history, tables without primary keys |
| [Change Tracking](http://go.estuary.dev/source-sqlserver-ct) | Change tracking | Real-time | No | Computed columns, lower storage overhead |
| [Batch](http://go.estuary.dev/source-sqlserver-batch) | Periodic polling | Minutes to hours | Yes | Views, custom queries, minimal setup |

\* CDC can capture from a read replica, but the CDC worker must run on the primary instance.

**Choose CDC when:**

- You need to capture tables without a primary key
- You need complete audit logging with full row history (CT may combine intermediate
  changes when they occur in rapid succession)

**Choose Change Tracking when:**

- You need to capture computed columns or computed primary keys (CDC cannot capture these)
- You want lower storage overhead on the source database (CT stores only primary keys, not full row contents)
- Your tables all have primary keys

**Choose Batch when:**

- Your SQL Server instance doesn't support CDC or Change Tracking
- You need to capture from database views
- You want to execute custom or ad-hoc queries

## Supported versions and platforms

This connector will work on both hosted deployments and all major cloud providers. It is designed for databases using any version of SQL Server which has CDC support, and is regularly tested against SQL Server 2017 and up.

Setup instructions are provided for the following platforms:

- [Self-hosted SQL Server](#self-hosted-sql-server)
- [Azure SQL Database](#azure-sql-database)
- [Amazon RDS for SQL Server](./amazon-rds-sqlserver/)
- [Google Cloud SQL for SQL Server](./google-cloud-sql-sqlserver/)

## Prerequisites

To capture change events from SQL Server tables using this connector, you need:

- For each table to be captured, a primary key should be specified in the database.
  If a table doesn't have a primary key, you must manually specify a key in the associated Estuary collection definition while creating the capture.
  [See detailed steps](#specifying-estuary-collection-keys).

- [CDC enabled](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/enable-and-disable-change-data-capture-sql-server?view=sql-server-ver16)
  on both the database and the individual tables to be captured.
  - Enabling CDC on a source table create a _change table_ in the database, from which the connector reads. You may optionally enable the "Automatic Capture Instance Management" advanced option to have the connector manage these automatically, but this will require additional permissions. See [Automatic Capture Instance Management](#automatic-capture-instance-management) for more details.

- A user role with:
  - The `VIEW DATABASE STATE` or (in newer versions of SQL Server) `VIEW DATABASE PERFORMANCE STATE` permission.
  - `SELECT` permissions on the CDC schema and the schemas that contain tables to be captured.
  - Access to the change tables created as part of the SQL Server CDC process.

## Setup

To meet these requirements, follow the steps for your hosting type.

- [Self-hosted SQL Server](#self-hosted-sql-server)
- [Azure SQL Database](#azure-sql-database)
- [Amazon RDS for SQL Server](./amazon-rds-sqlserver/)
- [Google Cloud SQL for SQL Server](./google-cloud-sql-sqlserver/)

### Self-hosted SQL Server

1. Connect to the server and issue the following commands:

```sql
USE <database>;
-- Enable CDC for the database.
EXEC sys.sp_cdc_enable_db;
-- Create user and password for use with the connector.
CREATE LOGIN flow_capture WITH PASSWORD = 'secret';
CREATE USER flow_capture FOR LOGIN flow_capture;
-- Grant the user permissions on the CDC schema and schemas with data.
-- This assumes all tables to be captured are in the default schema, `dbo`.
-- Add similar queries for any other schemas that contain tables you want to capture.
GRANT SELECT ON SCHEMA :: dbo TO flow_capture;
GRANT SELECT ON SCHEMA :: cdc TO flow_capture;
-- Grant the 'VIEW DATABASE STATE' permission.
GRANT VIEW DATABASE STATE TO flow_capture;
-- Enable CDC on tables. The below query enables CDC on table 'dbo.foobar',
-- you should add similar query for all other tables you intend to capture.
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'foobar', @role_name = 'flow_capture';
```

2. Allow secure connection to Estuary from your hosting environment. Either:

   - Set up an [SSH server for tunneling](/guides/connect-network/).

     When you fill out the [endpoint configuration](#endpoint),
     include the additional `networkTunnel` configuration to enable the SSH tunnel.
     See [Connecting to endpoints on secure networks](/concepts/connectors.md#connecting-to-endpoints-on-secure-networks)
     for additional details and a sample.

   - [Allowlist the Estuary IP addresses](/reference/allow-ip-addresses) in your firewall rules.

### Azure SQL Database

1. Allow connections between the database and Estuary. There are two ways to do this: by granting direct access to Estuary's IP or by creating an SSH tunnel.

   1. To allow direct access:

      - Create a new [firewall rule](https://learn.microsoft.com/en-us/azure/azure-sql/database/firewall-configure?view=azuresql#use-the-azure-portal-to-manage-server-level-ip-firewall-rules) that grants access to the [Estuary IP addresses](/reference/allow-ip-addresses).

   2. To allow secure connections via SSH tunneling:
      - Follow the guide to [configure an SSH server for tunneling](/guides/connect-network/)
      - When you configure your connector as described in the [configuration](#configuration) section above, including the additional `networkTunnel` configuration to enable the SSH tunnel. See [Connecting to endpoints on secure networks](/concepts/connectors.md#connecting-to-endpoints-on-secure-networks) for additional details and a sample.

2. In your SQL client, connect to your instance as the default `sqlserver` user and issue the following commands.

```sql
USE <database>;
-- Enable CDC for the database.
EXEC sys.sp_cdc_enable_db;
-- Create user and password for use with the connector.
CREATE LOGIN flow_capture WITH PASSWORD = 'secret';
CREATE USER flow_capture FOR LOGIN flow_capture;
-- Grant the user permissions on the CDC schema and schemas with data.
-- This assumes all tables to be captured are in the default schema, `dbo`.
-- Add similar queries for any other schemas that contain tables you want to capture.
GRANT SELECT ON SCHEMA :: dbo TO flow_capture;
GRANT SELECT ON SCHEMA :: cdc TO flow_capture;
-- Grant the 'VIEW DATABASE STATE' permission.
GRANT VIEW DATABASE STATE TO flow_capture;
-- Enable CDC on tables. The below query enables CDC on table 'dbo.foobar',
-- you should add similar query for all other tables you intend to capture.
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'foobar', @role_name = 'flow_capture';
```

3. Note the following important items for configuration:

   - Find the instance's host under Server Name. The port is always `1433`. Together, you'll use the host:port as the `address` property when you configure the connector.

### IAM Authentication

For databases hosted on Amazon RDS, you can authenticate with an AWS IAM role
instead of a password. This requires an RDS Proxy in front of the database; see
[Amazon RDS for SQL Server](./amazon-rds-sqlserver/#iam-authentication) for
setup instructions.

For Azure SQL Database, you can authenticate with an Azure App Registration
instead of a password.

Follow the steps in the [Azure IAM guide][azure-iam] to create an App
Registration and make note of the Application ID and Tenant ID to use when
configuring the connector's authentication options.

Ensure that the SQL logical server has Entra authentication enabled and connect
to the Azure SQL Database as the Entra admin. This can be done from the
Database Query Editor. Run the following commands to create a user for the App
Registration, granting it the same permissions as the `flow_capture` user in
the [Azure SQL Database](#azure-sql-database) setup instructions above:

```sql
CREATE USER [my-app-registration-name] FROM EXTERNAL PROVIDER;
GRANT SELECT ON SCHEMA :: dbo TO [my-app-registration-name];
GRANT SELECT ON SCHEMA :: cdc TO [my-app-registration-name];
GRANT VIEW DATABASE STATE TO [my-app-registration-name];
```

When enabling CDC on tables, use the App Registration name as the gating
`role_name` argument, or grant the App Registration membership in whichever
gating role your capture instances already use.

[azure-iam]: /guides/iam-auth/azure/

### Handling DDL Alterations to Source Tables

In SQL Server, adding a column to the source table will not automatically cause it to be added to the CDC change table. Instead [Microsoft's recommended approach](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-data-capture-sql-server?view=sql-server-ver17#handling-changes-to-source-table) is to create a second capture instance which reflects the new state of the source table, transition over to the new instance, and then delete the old one.

The connector will automatically detect the existence of a second capture instance and will seamlessly switch over to the newest one as soon as it reaches a point in the event stream where they are both valid. After this switchover occurs you may delete the old instance.

If you are managing capture instances manually, you will need to manually create the new instance with `sys.sp_cdc_enable_table`, wait for the new column to begin being captured, and then delete the old instance at your leisure using `sys.sp_cdc_disable_table`.

If you are using [Automatic Capture Instance Management](#automatic-capture-instance-management), the connector detect DDL alterations to the source table and will handle the whole process of creating a new CDC instance and dropping the old one automatically.

### Automatic Capture Instance Management

You may wish to have the connector automatically issue `sys.sp_cdc_enable_table` (and sometimes also `sys.sp_cdc_disable_table`) statements on your behalf. This can be done by enabling the option `Advanced > Automatic Capture Instance Management` and granting the required permission if necessary.

Unfortunately, the `sys.sp_cdc_enable_table` stored procedure [requires membership in the `db_owner` database role](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-enable-table-transact-sql?view=sql-server-ver17#permissions) to call.

Granting that permission to the capture user is a hard requirement for using this feature, and can be done by executing `ALTER ROLE db_owner ADD MEMBER <user>` on the source database.

### Automatic Change Table Cleanup

By default, CDC change tables will retain change events for [three days](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-add-job-transact-sql?view=sql-server-ver17#----retention). This can be modified via `sys.sp_cdc_change_job`, but a lower retention period can put you at a higher risk of losing change events in the event of downtime, if the issue persists long enough for unconsumed change events to expire. If this happens a complete backfill will be required to re-establish consistency.

An alternative solution is to enable the option `Advanced > Automatic Change Table Cleanup`. When this option is enabled, the connector will manually remove change events from the relevant change tables once it receives confirmation that they have been durably persisted into an Estuary collection.

Unfortunately, the `sys.sp_cdc_cleanup_change_table` stored procedure used to delete CDC events from the change table [requires membership in the `db_owner` database role](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-cleanup-change-table-transact-sql?view=sql-server-ver17#permissions) to call.

Granting that permission to the capture user is a hard requirement for using this feature, and can be done by executing `ALTER ROLE db_owner ADD MEMBER <user>` on the source database.

Note also that you still need to have enough free storage space to hold the full 3 days of CDC event retention in the worst case. Otherwise, in the event of downtime exceeding what you can store, you have only changed the failure mode to running out of disk space. This option is best used in situations where it is _possible_ to use that much storage but it is still undesirable for other reasons to use that much in normal operation, such as when using flexible storage volumes on a cloud host.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file.
See [connectors](/concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the SQL Server source connector.

### Properties

#### Endpoint

| Property                        | Title               | Description                                                                                                                                 | Type    | Required/Default           |
| ------------------------------- | ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- | ------- | -------------------------- |
| **`/address`**                  | Server Address      | The host or host:port at which the database can be reached.                                                                                 | string  | Required                   |
| **`/database`**                 | Database            | Logical database name to capture from.                                                                                                      | string  | Required                   |
| **`/user`**                     | User                | The database user to authenticate as.                                                                                                       | string  | Required, `"flow_capture"` |
| `/historyMode` | History Mode | Capture each change event, without merging. | boolean | `false` |

##### Authentication

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| **`/credentials`** | Authentication | Authentication method and credentials that provide access to the database. | object | Required |
| `/credentials/auth_type` | Auth Type | The authentication method to use. One of `UserPassword`, `AWSIAM`, or `AzureIAM`. | string |  |
| `/credentials/password` | Password | Password for the specified database user. | string | Required for `UserPassword` auth |
| `/credentials/aws_region` | AWS Region | AWS region of your resource. | string | Required for `AWSIAM` auth |
| `/credentials/aws_role_arn` | AWS Role ARN | AWS role for Estuary to use that has access to the resource. | string | Required for `AWSIAM` auth |
| `/credentials/azure_client_id` | Azure Client ID | Application (client) ID of the App Registration. | string | Required for `AzureIAM` auth |
| `/credentials/azure_tenant_id` | Azure Tenant ID | Directory (tenant) ID of the App Registration. | string | Required for `AzureIAM` auth |

##### Discovery Filters

Options that restrict which tables are surfaced by discovery. These take effect
when discovery runs. If your capture has automatic discovery enabled, a table
these filters exclude will be deactivated the next time discovery runs.

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| `/discoveryFilters` | Discovery Filters | Options that restrict which tables are visible to discovery. | object |  |
| `/discoveryFilters/include_schemas` | Include Schemas | If specified, only tables in the listed schemas are discovered. | string array |  |
| `/discoveryFilters/exclude_schemas` | Exclude Schemas | Tables in the listed schemas are excluded from discovery. | string array |  |
| `/discoveryFilters/table_patterns` | Table Patterns | If specified, only tables matching at least one of these glob patterns are discovered. A pattern containing a `.` matches against the qualified `schema.table` name. A pattern without a `.` matches the unqualified table name in any schema. Use `*` or `?` as wildcards. | string array |  |
| `/discoveryFilters/discover_only_enabled` | Discover Only CDC-Enabled Tables | When set, the connector only discovers tables which already have CDC capture instances enabled. Combined as a union with the equivalent setting under Advanced Options. | boolean |  |

##### Advanced options

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| `/advanced`                     | Advanced Options    | Options for advanced users. You should not typically need to modify these.                                                                  | object  |                            |
| `/advanced/backfill_chunk_size` | Backfill Chunk Size | The number of rows which should be fetched from the database in a single backfill query.                                                    | integer | `4096`                     |
| `/advanced/skip_backfills`      | Skip Backfills      | A comma-separated list of fully-qualified table names which should not be backfilled.                                                       | string  |                            |
| `/advanced/source_tag` | Source Tag | This value is added as the property 'tag' in the source metadata of each document. | string |  |
| `/advanced/rediscovery_interval` | Rediscovery Interval | How often the connector re-runs discovery while a capture is running, in order to notice schema changes and newly added tables. Accepts duration strings like `15m` or `1h`, from `1m` up to `8760h`. | string | `"15m"` |

#### Bindings

| Property         | Title               | Description                                                                                                                                                                  | Type                                                          | Required/Default |
| ---------------- | ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------- | ---------------- |
| **`/namespace`** | Namespace           | The [namespace/schema](https://learn.microsoft.com/en-us/sql/relational-databases/databases/databases?view=sql-server-ver16#basic-information-about-databases) of the table. | string                                                        | Required         |
| **`/stream`**    | Stream              | Table name.                                                                                                                                                                  | string                                                        | Required         |
| `/primary_key`   | Primary Key Columns | The columns which together form the primary key of the table.                                                                                                                | array                                                          |                  |
| `/mode` | [Backfill Mode](/reference/backfilling-data/#resource-configuration-backfill-modes) | How the preexisting contents of the table should be backfilled. This should generally not be changed. | string | `""` |
| `/priority` | Backfill Priority | Optional priority for this binding. The highest priority binding(s) will be backfilled completely before any others. Negative priorities are allowed and will cause a binding to be backfilled after others. | integer | `0` |
| `/advanced/additional_backfill_filter` | Additional Backfill Filter | Optional filter clause which will be applied to all backfill queries for this binding. Contact Estuary support for assistance before using this option. | string | |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: "ghcr.io/estuary/source-sqlserver:v0"
        config:
          address: "<host>:1433"
          database: "my_db"
          user: "flow_capture"
          credentials:
            auth_type: UserPassword
            password: "secret"
    bindings:
      - resource:
          stream: ${TABLE_NAME}
          namespace: dbo
          primary_key: ["id"]
        target: ${PREFIX}/${COLLECTION_NAME}
```

To authenticate to an Azure SQL Database with [Azure IAM](#iam-authentication)
instead, replace the credentials block:

```yaml
          credentials:
            auth_type: AzureIAM
            azure_client_id: "11111111-2222-3333-4444-555555555555"
            azure_tenant_id: "66666666-7777-8888-9999-000000000000"
```

Your capture definition will likely be more complex, with additional bindings for each table in the source database.

[Learn more about capture definitions.](/concepts/captures.md)

## Specifying Estuary collection keys

Every Estuary collection must have a [key](/concepts/collections.md#keys).
As long as your SQL Server tables have a primary key specified, the connector will set the
corresponding collection's key accordingly.

In cases where a SQL Server table you want to capture doesn't have a primary key,
you can manually add it to the collection definition during the [capture creation workflow](/guides/create-dataflow.md#create-a-capture).

1. After you input the endpoint configuration and click **Next**,
   the tables in your database have been mapped to Estuary collections.
   Click each collection's **Specification** tab and identify a collection where `"key": [ ],` is empty.

2. Click inside the empty key value in the editor and input the name of column in the table to use as the key, formatted as a JSON pointer. For example `"key": ["/foo"],`

   Make sure the key field is required, not nullable, and of an [allowed type](/concepts/collections.md#schema-restrictions).
   Make any other necessary changes to the [collection specification](/concepts/collections.md#specification) to accommodate this.

3. Repeat with other missing collection keys, if necessary.

4. Save and publish the capture as usual.
