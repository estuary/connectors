---
description: Capture Neon PostgreSQL data with Estuary's CDC connector. Setup guide includes logical replication, WAL handling, replication slots, publications, watermarks tables, and backfills.
---

# Neon PostgreSQL

Neon's logical replication feature allows you to replicate data from your Neon Postgres database to external destinations.

This connector is a variant of the [PostgreSQL connector](./PostgreSQL.md).
Refer to that page for additional connector features, usage, and the full
configuration reference. Information specific to Neon and its setup is
presented below.

## Prerequisites

- An [Estuary account](https://dashboard.estuary.dev/register) (start free, no credit card required)
- A [Neon account](https://console.neon.tech/)
- A **direct connection string** to your Neon database (not a pooled connection). See [Connection Pooling](#connection-pooling) below.

## Setup

### 1. Enable Logical Replication in Neon

Enabling logical replication modifies the Postgres `wal_level` configuration parameter, changing it from `replica` to `logical` for all databases in your Neon project. Once the `wal_level` setting is changed to `logical`, it cannot be reverted. Enabling logical replication also restarts all computes in your Neon project, meaning active connections will be dropped and have to reconnect.

To enable logical replication in Neon:

1. Select your project in the Neon Console.
2. On the Neon **Dashboard**, select **Project settings**.
3. Select **Beta**.
4. Click **Enable** to enable logical replication.

You can verify that logical replication is enabled by running the following query from the [Neon SQL Editor](https://neon.tech/docs/get-started-with-neon/query-with-neon-sql-editor):

```sql
SHOW wal_level;
 wal_level
-----------
 logical
```

### 2. Create a Postgres Role for Replication

It is recommended that you create a dedicated Postgres role for replicating data. The role must have the `REPLICATION` privilege.
The default Postgres role created with your Neon project and roles created using the Neon Console, CLI, or API are granted membership in the neon_superuser role, which has the required `REPLICATION` privilege.

To create a role in the Neon Console:

1. Navigate to the [Neon Console](https://console.neon.tech).
2. Select a project.
3. Select **Roles**.
4. Select the branch where you want to create the role.
5. Click **New Role**.
6. In the role creation dialog, specify a role name.
7. Click **Create**. The role is created and you are provided with the password for the role.

Alternatively, the following CLI command creates a role. To view the CLI documentation for this command, see [Neon CLI commands — roles](https://api-docs.neon.tech/reference/createprojectbranchrole).

```bash
neon roles create --name <role>
```

As a third option, the following Neon API method also creates a role. To view the API documentation for this method, refer to the Neon API reference.

```bash
curl 'https://console.neon.tech/api/v2/projects/hidden-cell-763301/branches/br-blue-tooth-671580/roles' \
  -H 'Accept: application/json' \
  -H "Authorization: Bearer $NEON_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{
  "role": {
    "name": "cdc_role"
  }
}' | jq
```

### 3. Grant Schema Access to Your Postgres Role

If your replication role does not own the schemas and tables you are replicating from, make sure to grant access. Run this commands for each schema:

```sql
GRANT pg_read_all_data TO cdc_role;
```

### 4. Create the watermarks table, grant privileges, and create publication:

```sql
CREATE TABLE IF NOT EXISTS public.flow_watermarks (slot TEXT PRIMARY KEY, watermark TEXT);
GRANT ALL PRIVILEGES ON TABLE public.flow_watermarks TO flow_capture;
CREATE PUBLICATION flow_publication;
ALTER PUBLICATION flow_publication SET (publish_via_partition_root = true);
ALTER PUBLICATION flow_publication ADD TABLE public.flow_watermarks, <other_tables>;
```

The `publish_via_partition_root`
  setting is recommended (because most users will want changes to a partitioned table to be captured
  under the name of the root table) but is not required.

Refer to the [Postgres docs](https://www.postgresql.org/docs/current/sql-alterpublication.html) if you need to add or remove tables from your publication. Alternatively, you also can create a publication `FOR ALL TABLES`.

Upon start-up, the Estuary connector for Postgres will automatically create the [replication slot](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html#LOGICALDECODING-REPLICATION-SLOTS) required for ingesting data change events from Postgres. The slot's name will be prefixed with `estuary_`, followed by a unique identifier.

To prevent storage bloat, **Neon automatically removes _inactive_ replication slots after a period of time if there are other _active_ replication slots**. If you have or intend on having more than one replication slot, please see [Unused replication slots](https://neon.tech/docs/guides/logical-replication-neon#unused-replication-slots) to learn more.

## Connection Pooling

This capture connector requires a direct connection to your Neon database. Neon's connection pooler does not support the PostgreSQL replication protocol required for CDC captures.

Neon provides two types of connection strings:
- **Pooled** (hostname contains `-pooler`): e.g., `ep-cool-darkness-123456-pooler.us-east-2.aws.neon.tech` — **will not work**
- **Direct** (no `-pooler`): e.g., `ep-cool-darkness-123456.us-east-2.aws.neon.tech` — **use this one**

To get your direct connection string:
1. Go to your Neon project **Dashboard**
2. Open the **Connection Details** widget
3. Ensure **Connection pooling** is toggled **off**
4. Copy the connection string

If you use a pooled connection, the capture will fail because the pooler does not pass through replication commands.

## Allow Inbound Traffic

If you are using Neon's **IP Allow** feature to limit the IP addresses that can connect to Neon, you will need to allow inbound traffic from Estuary's IP addresses.
Refer to the [Estuary documentation](/reference/allow-ip-addresses) for the list of IPs that need to be allowlisted for your data plane.
For information about configuring allowed IPs in Neon, see [Configure IP Allow](https://neon.tech/docs/introduction/ip-allow).

## Create a Postgres Source Connector in Estuary

1. In the Estuary web UI, select **Sources** from the left navigation bar and click **New Capture**.
2. In the connector catalog, choose **Neon PostgreSQL** and click **Connect**.
3. Enter the connection details for your Neon database. You can get these details from your Neon connection string, which you'll find in the **Connection Details** widget on the **Dashboard** of your Neon project. **Make sure connection pooling is toggled off** so you get a direct connection string. It will look like this:

   ```bash
   postgres://cdc_role:AbC123dEf@ep-cool-darkness-123456.us-east-2.aws.neon.tech/dbname?sslmode=require
   ```

   :::warning
   Do not use a pooled connection string (hostname containing `-pooler`). See [Connection Pooling](#connection-pooling).
   :::

   Enter the details for **your connection string** into the source connector fields. Based on the sample connection string above, the values would be specified as shown below. Your values will differ.

   - Name: Name of the Capture connector
   - Server Address: ep-cool-darkness-123456.us-east-2.aws.neon.tech:5432
   - User: cdc_role
   - Password: `AbC123dEf` in the example, or your own value based on the connection string.
   - Database: dbname

3. Click **Next**. Estuary will now scan the source database for all the tables that can be replicated. Select one or more table(s) by checking the checkbox next to their name.
Optionally, you can change the name of the destination name for each table. You can also take a look at the schema of each stream by clicking on the **Collection** tab.

4. Click **Save and Publish** to provision the connector and kick off the automated backfill process.

### Sample

A minimal capture definition will look like the following:

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-neon-postgres:v3
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
