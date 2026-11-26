---
description: Capture IBM Db2 inserts, updates, and deletes into Estuary using SQL Replication.
---

# IBM Db2

This connector backfills existing rows and continuously captures inserts,
updates, and deletes from IBM Db2 for Linux, UNIX, and Windows (LUW).
The tested version range is **12.1**, including Community 12.1.5.0.
Db2 for i, z/OS, and pureScale/DPF-specific behavior are outside its scope.
For periodic queries, see the [Db2 Batch Query Connector](./db2-batch.md).

## Before you begin

You need a Db2 LUW server, an instance administrator, archive and backup
storage, and network access from Estuary (directly or through an
[SSH tunnel](/guides/connect-network/)). SQL Replication and its `asnclp`,
`asncap`, and `asnccmd` commands are available with the Db2 server install;
make sure they are installed and on the instance account's path.

These terms describe the setup:

- **SQL Replication** copies committed database changes into ordinary SQL tables.
- **ASN Capture (`asncap`)** is the server process that reads the transaction log
  and publishes those changes. It must keep running.
- A **CD table** (change-data table) stores the published changes for one source table.
- A **registration** links a source table to its CD table and specifies which
  columns and old values to record. Its `STATE` must be `A` (active).
- The global **SYNCHPOINT** is the commit sequence through which every CD table
  has been published. The connector uses it as its safe read frontier.

The connector reads source tables, CD tables, and catalogs. Its only writes
are to a registered watermarks table used to coordinate backfill and streaming.
An administrator performs the following setup; the connector does not create
registrations or modify replication control tables.

## Setup

Examples use database `TESTDB`, source schema `DB2INST1`, and capture schema
`ASN`. Substitute your names and run commands as the instance owner.

### 1. Enable archive logging and create control tables

Choose archive and backup directories, then follow your database's maintenance
procedure for enabling logging and taking an initial full backup:

```bash
db2 "UPDATE DB CFG FOR TESTDB USING LOGARCHMETH1 DISK:/db2/archive"
db2 "BACKUP DB TESTDB TO /db2/backup"
db2 "CONNECT TO TESTDB"
```

If capture control tables do not exist, save this as `control.asnclp` and run
`asnclp -f control.asnclp`:

```text
SET SERVER CAPTURE TO DB TESTDB;
SET RUN SCRIPT NOW STOP ON SQL ERROR ON;
CREATE CONTROL TABLES FOR CAPTURE SERVER;
```

### 2. Register source tables and watermarks

Enable data capture and create the watermark table using the Db2 SQL client:

```sql
ALTER TABLE DB2INST1.ORDERS DATA CAPTURE CHANGES;
ALTER TABLE DB2INST1.LINES DATA CAPTURE CHANGES;
CREATE TABLE DB2INST1.FLOW_WATERMARKS (
    SLOT VARCHAR(1000) NOT NULL PRIMARY KEY,
    WATERMARK VARCHAR(1000)
) DATA CAPTURE CHANGES;
```

Save this as `register.asnclp` and run `asnclp -f register.asnclp`:

```text
SET SERVER CAPTURE TO DB TESTDB;
SET RUN SCRIPT NOW STOP ON SQL ERROR ON;
CREATE REGISTRATION (DB2INST1.ORDERS) DIFFERENTIAL REFRESH STAGE CDORDERS COLS ALL IMAGE BOTH PREFIX X;
CREATE REGISTRATION (DB2INST1.LINES) DIFFERENTIAL REFRESH STAGE CDLINES OPTIONS UPDATE AS DELETE INSERT ON;
CREATE REGISTRATION (DB2INST1.FLOW_WATERMARKS) DIFFERENTIAL REFRESH STAGE CDFLOW_WATERMARKS;
```

Use **before-images** (`IMAGE BOTH PREFIX X`) when you want old column values
in the same CD row: `XID` holds the old `ID`, for example. Include before-images
for every collection-key column, including custom keys. Use **UPDATE AS DELETE
INSERT** when before-images are unavailable or you prefer that registration
style: each update produces adjacent delete and insert rows. The connector
reconstructs both styles as one update when the key is unchanged, or a delete
and insert when it changes. Discovery recommends the source primary key.

Set `advanced.watermarksTable` to `DB2INST1.FLOW_WATERMARKS`. Its default is
`<USER>.FLOW_WATERMARKS`, with the endpoint user uppercased.

### 3. Grant the connector access

Create the endpoint user through your OS or LDAP user management, then grant:

```sql
GRANT CONNECT ON DATABASE TO USER flow_capture;
GRANT SELECT ON TABLE SYSIBMADM.DBCFG TO USER flow_capture;
GRANT SELECT ON TABLE ASN.IBMSNAP_REGISTER TO USER flow_capture;
GRANT SELECT ON TABLE ASN.IBMSNAP_CAPPARMS TO USER flow_capture;
GRANT SELECT ON TABLE DB2INST1.ORDERS TO USER flow_capture;
GRANT SELECT ON TABLE DB2INST1.LINES TO USER flow_capture;
GRANT SELECT, INSERT, UPDATE ON TABLE DB2INST1.FLOW_WATERMARKS TO USER flow_capture;
```

CD tables may have a different schema. Find their actual names and generate
SELECT grants with this query, then execute the returned statements:

```sql
SELECT SOURCE_TABLE, CD_OWNER, CD_TABLE,
       'GRANT SELECT ON TABLE "' || RTRIM(CD_OWNER) || '"."'
       || RTRIM(CD_TABLE) || '" TO USER flow_capture;' AS GRANT_SQL
FROM ASN.IBMSNAP_REGISTER
WHERE SOURCE_OWNER = 'DB2INST1'
  AND SOURCE_TABLE IN ('ORDERS', 'LINES', 'FLOW_WATERMARKS');
```

The user also needs SELECT on `SYSCAT.TABLES`, `SYSCAT.COLUMNS`,
`SYSCAT.DATATYPES`, `SYSCAT.KEYCOLUSE`, and `SYSCAT.TABCONST`, normally supplied
by public catalog privileges. No write access to ASN control tables is needed.

### 4. Start ASN Capture and keep it running

For a two-second publication interval:

```sql
UPDATE ASN.IBMSNAP_CAPPARMS SET COMMIT_INTERVAL = 2, SLEEP_INTERVAL = 1;
```

Run `asncap capture_server=TESTDB capture_schema=ASN startmode=warmsi` under the
instance account. Use your service supervisor to start it after Db2 and restart
it if it fails. The following **systemd example** assumes an existing Linux
instance at `/home/db2inst1`; adapt paths and startup dependencies to your host:

```ini
[Unit]
Description=Db2 SQL Replication capture for TESTDB
After=network.target

[Service]
Type=simple
User=db2inst1
WorkingDirectory=/home/db2inst1
ExecStart=/bin/bash -c '. /home/db2inst1/sqllib/db2profile; exec asncap capture_server=TESTDB capture_schema=ASN startmode=warmsi'
ExecStop=/bin/bash -c '. /home/db2inst1/sqllib/db2profile; asnccmd capture_server=TESTDB capture_schema=ASN stop'
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Save it as `/etc/systemd/system/db2-asncap.service`, start the Db2 instance,
and run `systemctl daemon-reload` and `systemctl enable --now db2-asncap`.
Confirm `ASN0109I` in the capture log. Without an Apply program sending CAPSTART
signals, registrations can remain inactive after a cold start. The administrator
can activate the intended registrations and reload them:

```sql
UPDATE ASN.IBMSNAP_REGISTER SET STATE = 'A'
WHERE SOURCE_OWNER = 'DB2INST1'
  AND SOURCE_TABLE IN ('ORDERS', 'LINES', 'FLOW_WATERMARKS');
```

```bash
asnccmd capture_server=TESTDB capture_schema=ASN reinit
```

### 5. Prove that committed changes are published

Write a new probe (commit it if your SQL client disables autocommit):

```sql
MERGE INTO DB2INST1.FLOW_WATERMARKS T
USING (VALUES ('estuary-setup', VARCHAR(CURRENT TIMESTAMP))) S (SLOT, WATERMARK)
ON T.SLOT = S.SLOT
WHEN MATCHED THEN UPDATE SET WATERMARK = S.WATERMARK
WHEN NOT MATCHED THEN INSERT (SLOT, WATERMARK) VALUES (S.SLOT, S.WATERMARK);
```

Use the actual watermarks CD schema from step 3 in this check:

```sql
SELECT COUNT(*) AS PUBLISHED
FROM DB2INST1.CDFLOW_WATERMARKS C, DB2INST1.FLOW_WATERMARKS W,
     ASN.IBMSNAP_REGISTER R
WHERE R.GLOBAL_RECORD = 'Y'
  AND W.SLOT = 'estuary-setup' AND C.SLOT = W.SLOT
  AND C.WATERMARK = W.WATERMARK AND C.IBMSNAP_OPERATION IN ('I', 'U')
  AND C.IBMSNAP_COMMITSEQ <= R.SYNCHPOINT;
```

Expected result: **`PUBLISHED = 1`**, usually after a few publication intervals.
This proves the new change reached its CD table and the global safe frontier.
If it stays zero, check the service logs and registration state before creating
the Estuary capture. A single CD table's maximum sequence is not a safe frontier.

## Configuration

Configure a capture in the Estuary web app or with
[flowctl](/guides/flowctl/). Discovery lists actively registered user tables and
excludes system schemas, the capture schema, CD tables, and the watermarks
table. Db2 unquoted identifiers are usually uppercase; use the exact names
returned by discovery. Recommended collection names also preserve case, so
tables such as `Mixed.Orders` and `Mixed.ORDERS` have distinct targets.

### Endpoint properties

| Property | Description | Type | Required/Default |
|----------|-------------|------|------------------|
| **`/address`** | Host or host:port. The default port is `50000`. | string | Required |
| `/user` | Database user. | string | `"flow_capture"` |
| **`/password`** | Database user's password. | string | Required, secret |
| **`/database`** | Database name. | string | Required |
| `/historyMode` | Capture change history without reducing it to a final state. | boolean | `false` |
| `/advanced/capture_schema` | Schema containing SQL Replication control tables. | string | `"ASN"` |
| `/advanced/watermarksTable` | Schema-qualified, registered watermarks table. | string | `<USER>.FLOW_WATERMARKS` (uppercased user) |
| `/advanced/backfill_chunk_size` | Rows fetched in each backfill query. | integer | `50000` |
| `/advanced/polling_interval` | Positive duration between CD polls, such as `1s` or `30s`. | string | `"1s"` |
| `/advanced/discover_schemas` | Restrict discovery to these schemas. | array of strings | All user schemas |
| `/advanced/timezone` | Time zone used to interpret Db2 timestamps. | string | `"UTC"` |
| `/advanced/source_tag` | Optional value added to `/_meta/source/tag`. | string | Unset |
| `/advanced/feature_flags` | Internal options; change only with Estuary support. | string | Unset |
| `/networkTunnel` | SSH bastion connection settings. | object | Unset |

Db2 connection strings cannot carry `;` in the user, password, or database name,
or leading or trailing whitespace in the password. If the password contains
these characters, change the database user's password before configuring the
capture. Braces (`{`, `}`) and equals signs (`=`) are passed unchanged.

### Binding properties

| Property | Description | Type | Required/Default |
|----------|-------------|------|------------------|
| **`/namespace`** | Source schema as stored in Db2. | string | Required |
| **`/stream`** | Source table as stored in Db2. | string | Required |
| `/mode` | Backfill mode: automatic (empty), `Normal`, `Precise`, `Only Changes`, or `Without Primary Key`. | string | Automatic |
| `/priority` | Backfill higher-priority bindings first. | integer | `0` |
| `/advanced/additional_backfill_filter` | Optional SQL backfill filter; configure with Estuary support. | string | Unset |

See [SQL capture backfill modes](/reference/backfilling-data/#resource-configuration-backfill-modes)
before changing backfill settings. Automatic mode assesses the effective
collection key, including custom keys. Decimal, temporal, and text keys use
unfiltered (`Normal`) backfills because their encoded ordering may differ from
Db2's ordering. Every column in the effective key must be declared `NOT NULL`,
including custom collection keys and resource key overrides. Validation and table
activation reject nullable key columns: choose a `NOT NULL` key and re-backfill
affected bindings. Earlier versions could skip rows with null key values.

### Example

```yaml
captures:
  acme/db2:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-db2:v1
        config:
          address: db2.example.com:50000
          user: flow_capture
          password: <password>
          database: TESTDB
          advanced:
            capture_schema: ASN
            watermarksTable: DB2INST1.FLOW_WATERMARKS
    bindings:
      - resource:
          namespace: DB2INST1
          stream: ORDERS
        target: acme/orders
```

## Captured data

The connector initially backfills each table, then resumes from its committed
cursor on subsequent runs. Registered watermark writes coordinate the handover
between backfill and streaming. The primary key becomes the collection key;
tables without one use commit and intent metadata as a fallback event key.
Keyless backfills resume after the last scanned RID, so deleting an earlier
row does not skip unchanged rows in later chunks. Recovery reads also use the RID
for keyless backfills, even with a custom collection key.

Documents preserve Db2 column names and include `/_meta/op`: `c` for inserts
and backfill rows, `u` for updates, and `d` for deletes. A primary-key change
produces a delete at the old key followed by an insert at the new key. A
delete/insert update with an unchanged key produces a single update.

`/_meta/source` includes:

| Field | Meaning |
|-------|---------|
| `schema`, `table` | Source schema and table names. |
| `cursor` | Hexadecimal `IBMSNAP_COMMITSEQ`; empty for backfill rows. |
| `intent` | Hexadecimal `IBMSNAP_INTENTSEQ`, or a deterministic row offset during backfill. |
| `ts_ms` | Milliseconds since the Unix epoch from `IBMSNAP_LOGMARKER`, when available. |
| `snapshot` | `true` for backfill rows; otherwise absent. |
| `tag` | The configured source tag, when set. |

### Type mapping

Values are interpreted using source-table column types, even when CD-table
types differ. User-defined distinct types use their underlying source type.

| Db2 type | JSON representation |
|----------|---------------------|
| `BOOLEAN` | boolean |
| `SMALLINT`, `INTEGER`, `BIGINT` | integer |
| `REAL`, `DOUBLE` | number |
| `DECIMAL`, `DECFLOAT` | Numeric string to preserve precision |
| `CHAR`, `VARCHAR`, `LONG VARCHAR`, `CLOB`, `GRAPHIC`, `VARGRAPHIC`, `LONG VARGRAPHIC`, `DBCLOB`, `XML` | string |
| `BINARY`, `VARBINARY`, `BLOB`, character types `FOR BIT DATA` | Base64-encoded string |
| `DATE` | `YYYY-MM-DD` string |
| `TIME` | `HH:MM:SS` string, including `24:00:00` |
| `TIMESTAMP` | RFC3339 string with all fractional digits preserved, interpreted in `advanced.timezone` |

SQL `NULL` is emitted as JSON `null`. Fixed-width character padding is
preserved. LOB and XML reads check the database length (after serialization to a
CLOB for XML) and recover non-null values in small pieces if the client driver
reports null. A failed recovery stops capture with the table, row key or RID,
and column in the error. Re-backfill data captured by earlier versions if non-null
LOBs or XML were emitted as null at 65,535 bytes or subsequent 65,536-byte intervals.

GRAPHIC, VARGRAPHIC, LONG VARGRAPHIC, and DBCLOB values are converted
to CLOBs in SQL so the driver preserves empty strings, embedded NULs, and Unicode
contents, including in CDC before-images. Source columns omitted from a CD registration,
such as unregistered LOB columns, are omitted from streamed changes, with a warning
logged once per table activation. Registered `CLOB`, `BLOB`, `DBCLOB`, and `XML` columns are also
omitted when their CD columns contain change flags instead of the original LOB
or XML type. Backfill rows can still contain those source columns; subsequent
changes to their contents are not captured through change flags.

## Monitoring and recovery

To measure published capture lag for a table, substitute the saved hexadecimal
cursor from the connector's progress log and the registration's actual CD name:

```sql
SELECT COUNT(*) AS PENDING_CD_ROWS
FROM DB2INST1.CDORDERS C, ASN.IBMSNAP_REGISTER R
WHERE R.GLOBAL_RECORD = 'Y'
  AND C.IBMSNAP_COMMITSEQ > X'<saved-cursor>'
  AND C.IBMSNAP_COMMITSEQ <= R.SYNCHPOINT;
```

Zero means the connector is caught up with published changes for that table;
repeat for other bindings. Where `IBMSNAP_LOGMARKER` is available, also select
`MIN(C.IBMSNAP_LOGMARKER)` to see the timestamp of its oldest unread change.
This query excludes log work not yet published by asncap; use the step 5 probe
and capture service logs to check publication itself.

The timeout `ASN Capture (asncap) does not appear to be running or is not
capturing <watermarksTable>` means a fence was not published. It includes the
saved cursor, global SYNCHPOINT, watermark age, frontier idle duration, and
watermarks registration STATE. A poll started at least five minutes after the
write must see the watermark; it may finish draining its backlog first.
An idle poll also fails after five minutes without frontier movement while a
watermark is pending. Check asncap logs, CD access, and the watermark registration.

Registration states and CD column catalogs are checked at most once a minute,
before advancing the cursor. A stopped (`S`), inactive (`I`), or missing
registration stops capture. Have the administrator restore the registration,
activate it and run `asnccmd ... reinit` as in step 4, then verify publication.
Re-backfill the affected bindings before restarting: changes during the gap may
be missing. A re-registration that adds columns also requires a re-backfill.
The connector logs added CD columns and continues using its existing projection.

ASN Capture's `RETENTION_LIMIT` defaults to seven days. If it prunes CD rows
before the connector reads them, changes are lost. Size retention for expected
downtime. Re-backfill after a retention gap to recover current table contents;
expired change history cannot be recovered from CD tables. The connector does
not manage CD pruning or automatically recover every retention gap.
