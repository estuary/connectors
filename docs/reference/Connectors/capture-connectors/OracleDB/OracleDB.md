---
description: Capture OracleDB changes into Estuary with LogMiner for container or non-container databases. Setup guidance includes configuring dictionary modes and network tunnels.
---

import ReactPlayer from "react-player";

# OracleDB
This connector captures data from OracleDB into Estuary collections using [Oracle Logminer](https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html#GUID-2555A155-01E3-483E-9FC6-2BDC2D8A4093).

<ReactPlayer controls url="https://www.youtube.com/watch?v=mE7LFSqfwY8" />

## Features

This connector includes the following features:

| Feature | Availability | Notes |
| --- | --- | --- |
| Real-time capture | ✅ | Destination sync cadence can still be controlled on the materialization side |
| [SSH tunneling](/guides/connect-network) | ✅ | [Private and BYOC](/private-byoc) deployments can also support [reverse SSH](/guides/connect-network/#expose-ports-on-a-reverse-ssh-tunnel-bastion) |
| [Private networking](/private-byoc/privatelink) | ✅<br/>Only available for [private/BYOC](/private-byoc) deployments | Support for AWS PrivateLink, Azure Private Link, and GCP Private Service Connect |
| [Supports non-container instances](#non-container-databases) | ✅ | |
| [Supports container instances](#container-databases) | ✅ | |
| [Support for RAC](#oracle-rac) | ✅ | Automatic support for single- or multi-threaded use cases |
| [Automatic dictionary mode](#dictionary-modes) | ✅ | Swaps between efficient and more resource-intensive modes to handle schema changes |
| [History mode](/guides/customize-dataflows/#history-mode) | ✅ | |

## Prerequisites
* Oracle 11g or above
* Allow connections from Estuary to your Oracle database (if they exist in separate VPCs)
* Create a dedicated read-only Estuary user with access to all tables needed for replication

## Setup
Follow the steps below to set up the OracleDB connector.

### Create a Dedicated User

Creating a dedicated database user with read-only access is recommended for better permission control and auditing. Depending on whether your database is a container database (also known as CDB) or not, follow the corresponding section below.

## Non-container Databases

1. To create the user, run the following commands against your database:

```sql
CREATE USER estuary_flow_user IDENTIFIED BY <your_password_here>;
GRANT CREATE SESSION TO estuary_flow_user;
```

2. Next, grant the user read-only access to the relevant tables. The simplest way is to grant read access to all tables in the schema as follows:

```sql
GRANT SELECT ANY TABLE TO estuary_flow_user;
```

3. Alternatively, you can be more granular and grant access to specific tables in different schemas:

```sql
GRANT SELECT ON "<schema_a>"."<table_1>" TO estuary_flow_user;
GRANT SELECT ON "<schema_b>"."<table_2>" TO estuary_flow_user;
```

4. Create a watermarks table:
```sql
CREATE TABLE estuary_flow_user.FLOW_WATERMARKS(SLOT varchar(1000) PRIMARY KEY, WATERMARK varchar(4000));
```

5. Grant the user access to use logminer, read metadata from the database and write to the watermarks table:

```sql
GRANT SELECT_CATALOG_ROLE TO estuary_flow_user;
GRANT EXECUTE_CATALOG_ROLE TO estuary_flow_user;
GRANT SELECT ON V$DATABASE TO estuary_flow_user;
GRANT SELECT ON V$LOG TO estuary_flow_user;
GRANT LOGMINING TO estuary_flow_user;

GRANT INSERT, UPDATE ON estuary_flow_user.FLOW_WATERMARKS TO estuary_flow_user;
```

6. Enable supplemental logging:

For normal instances use:
```sql
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

For Amazon RDS instances use:
```sql
BEGIN rdsadmin.rdsadmin_util.alter_supplemental_logging(p_action => 'ADD', p_type   => 'ALL'); end;
```

7. Ensure user has quota on the USERS tablespace:

```sql
ALTER USER estuary_flow_user QUOTA UNLIMITED ON USERS;
```

## Container Databases

For working with container databases, access to the root container is necessary. Amazon RDS Oracle databases do not allow access to the root container and so they do not work if configured as a multi-tenant architecture database (whether single-tenant or multi-tenant).

1. To create a common user (requires `c##` prefix in the name of the user), run the following commands against your database:

```sql
CREATE USER c##estuary_flow_user IDENTIFIED BY <your_password_here> CONTAINER=ALL;
GRANT CREATE SESSION TO c##estuary_flow_user CONTAINER=ALL;
```

2. Next, grant the user read-only access to the relevant tables. The simplest way is to grant read access to all tables in the schema as follows:

```sql
GRANT SELECT ANY TABLE TO c##estuary_flow_user CONTAINER=ALL;
```

3. Alternatively, you can be more granular and grant access to specific tables in different schemas (run in the root container):

```sql
GRANT SELECT ON "<schema_a>"."<table_1>" TO c##estuary_flow_user CONTAINER=ALL;
GRANT SELECT ON "<schema_b>"."<table_2>" TO c##estuary_flow_user CONTAINER=ALL;
```

4. Create a watermarks table (the table should be in the PDB)
```sql
CREATE TABLE c##estuary_flow_user.FLOW_WATERMARKS(SLOT varchar(1000) PRIMARY KEY, WATERMARK varchar(4000));
GRANT INSERT, UPDATE ON c##estuary_flow_user.FLOW_WATERMARKS TO c##estuary_flow_user CONTAINER=ALL;
```

5. Finally you need to grant the user access to use logminer, read metadata from the database and write to the watermarks table:

```sql
GRANT SELECT_CATALOG_ROLE TO c##estuary_flow_user CONTAINER=ALL;
GRANT EXECUTE_CATALOG_ROLE TO c##estuary_flow_user CONTAINER=ALL;
GRANT LOGMINING TO c##estuary_flow_user CONTAINER=ALL;
GRANT ALTER SESSION TO c##estuary_flow_user CONTAINER=ALL;
GRANT SET CONTAINER TO c##estuary_flow_user CONTAINER=ALL;
```

5. Enable supplemental logging:

For normal instances use:
```sql
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

6. Ensure user has quota on the USERS tablespace:

```sql
ALTER USER c##estuary_flow_user QUOTA UNLIMITED ON USERS;
```

### Include Schemas for Discovery
In your Oracle configuration, you can specify the schemas that Estuary should look at when discovering tables. The schema names are case-sensitive. If the user does not have access to a certain schema, no tables from that schema will be discovered.

## Configuration
You configure connectors either in the Estuary web app, or by directly editing the catalog specification file. See [connectors](https://docs.estuary.dev/concepts/connectors/#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the OracleDB source connector.

To allow secure connections via SSH tunneling:
  * Follow the guide to [configure an SSH server for tunneling](/guides/connect-network/)
  * When you configure your connector as described in the [configuration](#configuration) section above, including the additional `networkTunnel` configuration to enable the SSH tunnel. See [Connecting to endpoints on secure networks](/concepts/connectors.md#connecting-to-endpoints-on-secure-networks) for additional details and a sample.

### Properties

#### Endpoint
| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| **`/address`** | Address | The host or host:port at which the database can be reached. | string | Required |
| **`/user`** | Username | The database user to authenticate as. | string | Required |
| **`/password`** | Password | Password for the specified database user. | string | Required |
| **`/database`** | Database | Logical database name to capture from. Defaults to ORCL. In multi-container environments use the PDB name. | string | Required |
| `/historyMode` | History Mode | Capture each change event, without merging. | boolean | `false` |

##### Advanced options

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| `/advanced/skip_backfills` | Skip Backfills | A comma-separated list of fully-qualified table names which should not be backfilled. | string |  |
| `/advanced/watermarksTable` | Watermarks Table | The name of the table used for watermark writes during backfills. Must be fully-qualified in `<schema>.<table>` form. | string  | `<USER>.FLOW_WATERMARKS` |
| `/advanced/backfill_chunk_size` | Backfill Chunk Size | The number of rows which should be fetched from the database in a single backfill query. | integer | `50000` |
| `/advanced/incremental_chunk_size` | Incremental Chunk Size | The number of rows which should be fetched from the database in a single incremental query. | integer | `10000` |
| `/advanced/dictionary_mode` | Dictionary Mode | How should dictionaries be used in Logminer: one of `online`, `extract`, or `smart`. When using online mode schema changes to the table may break the capture but resource usage is limited. When using extract mode schema changes are handled gracefully but more resources of your database (including disk) are used by the process. Defaults to smart which automatically switches between the two. | string | `smart` |
| `/advanced/discover_schemas` | Discover Schemas | If this is specified only tables in the selected schema(s) will be automatically discovered. Omit all entries to discover tables from all schemas. | string |  |
| `/advanced/node_id` | Node ID | Node ID for the capture. Each node in a replication cluster must have a unique 32-bit ID. The specific value doesn't matter so long as it is unique. If unset or zero the connector will pick a value. | integer |  |
| `/advanced/source_tag` | Source Tag | This value is added as the property 'tag' in the source metadata of each document. | string |  |
| `/advanced/rediscovery_interval` | Rediscovery Interval | How often the connector re-runs discovery while a capture is running, in order to notice schema changes and newly added tables. Accepts duration strings like `15m` or `1h`, from `1m` up to `8760h`. | string | `"15m"` |

##### Network tunnel

You may use `networkTunnel` properties with this capture to configure an SSH
tunnel. See [secure networks](/concepts/connectors/#connecting-to-endpoints-on-secure-networks)
for property names and usage.

#### Bindings

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| **`/namespace`** | Namespace | The [owner/schema](https://docs.oracle.com/database/121/CNCPT/intro.htm#CNCPT940) of the table. | string | Required |
| **`/stream`** | Stream | Table name. | string | Required |
| `/mode` | [Backfill Mode](/reference/backfilling-data/#resource-configuration-backfill-modes) | How the preexisting contents of the table should be backfilled. This should generally not be changed. | string | `""` |
| `/priority` | Backfill Priority | Optional priority for this binding. The highest priority binding(s) will be backfilled completely before any others. Negative priorities are allowed and will cause a binding to be backfilled after others. | integer | `0` |
| `/advanced/additional_backfill_filter` | Additional Backfill Filter | Optional filter clause which will be applied to all backfill queries for this binding. Contact Estuary support for assistance before using this option. | string | |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-oracle:v1
        config:
          address: database-1.abc.us-east-2.rds.amazonaws.com:1234
          user: "flow_capture"
          password: secret
          database: ORCL
          historyMode: false
          advanced:
            dictionary_mode: smart
          networkTunnel:
            sshForwarding:
              privateKey: -----BEGIN RSA PRIVATE KEY-----\n...
              sshEndpoint: ssh://ec2-user@19.220.21.33:22

    bindings:
      - resource:
          namespace: ${TABLE_NAMESPACE}
          stream: ${TABLE_NAME}
        target: ${PREFIX}/${COLLECTION_NAME}
```

## Dictionary Modes

Oracle writes redo log files using triplet object ID, data object ID and object versions to identify different objects in the database, rather than their name. This applies to table names as well as column names. When reading data from the redo log files using Logminer, a "dictionary" is used to translate the object identification data into user-facing names of those objects. When interacting with the database directly an _online_ dictionary, which is essentially the latest dictionary that knows how to translate currently existing table and column names is used by the database and by Logminer, however when capturing historical data, it is possible that the names of these objects or even their identifiers have changed (due to an `ALTER TABLE` statement for example). In these instances the _online_ dictionary will be insufficient for translating the object identifiers into names and Logminer will complain about a dictionary mismatch.

To resolve this issue, it is possible to _extract_ a dictionary into the redo log files themselves, so that when there are schema changes, Logminer can automatically handle using the appropriate dictionary for the time period an event is from. This operation however uses CPU and RAM, as well as consuming disk over time.

By default, Estuary's Oracle connector will automatically switch between these
two modes as needed. It starts in _online_ mode until it hits a dictionary
mismatch, switches to _extract_ mode just until covering the latest DDLs on all
tables, and then switches back to online mode for efficiency.

This _smart_ mode is the recommended dictionary mode for the Oracle connector.
However, you may also configure the dictionary mode as an advanced setting:

1. To extract the dictionary into the redo log files, the `extract` mode can be used. Be aware that this mode leads to more resource usage (CPU, RAM and disk).
2. To always use the online dictionary, the `online` mode can be used. This mode is more efficient, but it cannot handle schema changes in tables, so only use this mode with caution and when table schemas are known not to change.
3. To automatically switch between these two modes, use `smart` mode (the default mode). This is the default mode and combines efficiency with infrequent extraction when schemas change.

## Oracle RAC

This connector is compatible with Oracle RAC (Real Application Clusters)
deployments. With Oracle RAC, each instance writes its own redo thread.
Estuary's connector automatically handles multi-threaded Oracle deployments,
sorting SCN ranges across threads into global bounds.

No special configuration is required to select between single- or multi-threaded
use cases.

## Troubleshooting

1. If you see the following error when trying to connect:
```
ORA-01950: no privileges on tablespace 'USERS'
```

The SQL command below may resolve the issue:
```sql
ALTER USER estuary_flow_user QUOTA UNLIMITED ON USERS;
```

## Known Limitations

1. Table and column names longer than 30 characters are not supported by Logminer, and thus they are also not supported by this connector.
