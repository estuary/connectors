# IBM Db2 Change Data Capture Connector

This connector captures existing rows and subsequent inserts, updates, and
deletes from IBM Db2 for Linux, UNIX, and Windows (LUW) using SQL Replication.
See the [connector reference](../docs/reference/Connectors/capture-connectors/db2.md)
for customer setup and configuration.

## Capture behavior

The connector uses `sqlcapture` for backfill and checkpoint handling. Each poll
reads the global SQL Replication SYNCHPOINT, merges CD rows in binary
(COMMITSEQ, INTENTSEQ) order, and checkpoints only fully emitted transactions.
See the reference page for definitions, both registration styles, grants, and
a supervised asncap setup example. The connector writes only watermark rows.

Before-images or update-as-delete-insert registrations preserve key changes.
Automatic backfill mode checks the effective collection key; decimal, temporal,
and text keys use unfiltered backfills. Effective key columns must be `NOT NULL`;
choose a non-nullable key and re-backfill bindings previously using nullable keys.
Keyless scans resume and re-read values by RID, even with a custom collection key.
Source column types determine value conversion. Unregistered columns and CD
columns holding LOB/XML change flags are omitted from changes with a warning.
Added CD columns are logged; re-registering with new columns needs a re-backfill.
LOB and serialized XML lengths distinguish database nulls from a client driver
decoding bug. Non-null LOBs and XML misdecoded as null are re-read in small pieces;
failed recovery stops capture. Re-backfill affected data captured by earlier
versions at 65,535-byte LOB or XML lengths and subsequent 65,536-byte intervals.

A watermark must appear in a poll begun at least five minutes after its write,
or capture fails. An idle frontier has the same timeout while a watermark is
pending. Polls may finish draining their backlog before evaluating the deadline.
Registration and CD catalog checks run at most once a minute, before advancing
the cursor. Timeout diagnostics report the cursor, frontier, watermark age,
frontier idle duration, and watermark registration state.

ASN Capture manages CD retention (`RETENTION_LIMIT`, seven days by default).
Pruning unread changes loses data; re-backfill affected bindings after a gap.
Stopped or missing registrations likewise require administrator recovery and
a re-backfill. The connector does not prune CD tables.

## Development

The fixture pins `icr.io/db2_community/db2:12.1.5.0` (amd64), with a four GiB
memory limit and the disposable database `TESTDB`. Docker
must support privileged containers and amd64 emulation when running on an ARM
host. The credentials in the Compose file are made-up test credentials.

`tests/source-db2/setup.sh` starts the Compose service named `db`, waits for the
image's setup to finish, and runs `docker-initdb.sh`. The initializer performs
these setup steps:

1. Locate the single executable matching `/opt/ibm/db2/V*/instance/db2icrt` and
   run it with `-update-instance-if-exists -u db2fenc1 -nosharedgroup db2inst1`.
   This also repairs instance setup when the image leaves `sqllib` missing under
   amd64 emulation.
2. Run `db2start`, create `TESTDB`, set `LOGARCHMETH1` to `DISK:$HOME/arch`, and
   run `BACKUP DB TESTDB TO /dev/null`. Discarding the backup is appropriate only
   for this disposable fixture.
3. Create `DB2INST1.ORDERS`, `DB2INST1.LINES`, and
   `DB2INST1.FLOW_WATERMARKS` with primary keys and `DATA CAPTURE CHANGES`.
   Create `CDC_BI` and `CDC_DI` with `ID`, `NAME`, and `QTY` for live CDC tests.
   Also create `TEST_CAPTURE` with quoted lowercase `id` and `canary` columns
   and a DBCLOB `CONTENT` column for the shared end-to-end test catalog.
4. Run ASNCLP with `SET SERVER CAPTURE TO DB TESTDB` and
   `SET RUN SCRIPT NOW STOP ON SQL ERROR ON`, then:

   ```text
   CREATE CONTROL TABLES FOR CAPTURE SERVER;
   CREATE REGISTRATION (DB2INST1.ORDERS) DIFFERENTIAL REFRESH STAGE CDORDERS COLS ALL IMAGE BOTH PREFIX X;
   CREATE REGISTRATION (DB2INST1.LINES) DIFFERENTIAL REFRESH STAGE CDLINES OPTIONS UPDATE AS DELETE INSERT ON;
   CREATE REGISTRATION (DB2INST1.CDC_BI) DIFFERENTIAL REFRESH STAGE CDCDC_BI COLS ALL IMAGE BOTH PREFIX X;
   CREATE REGISTRATION (DB2INST1.CDC_DI) DIFFERENTIAL REFRESH STAGE CDCDC_DI OPTIONS UPDATE AS DELETE INSERT ON;
   CREATE REGISTRATION (DB2INST1.FLOW_WATERMARKS) DIFFERENTIAL REFRESH STAGE CDFLOW_WATERMARKS;
   CREATE REGISTRATION (DB2INST1.TEST_CAPTURE) DIFFERENTIAL REFRESH STAGE CDTEST_CAPTURE OPTIONS UPDATE AS DELETE INSERT ON;
   ```

5. Set `ASN.IBMSNAP_CAPPARMS.COMMIT_INTERVAL = 2` and `SLEEP_INTERVAL = 1`.
   Start `asncap capture_server=TESTDB capture_schema=ASN startmode=warmsi`
   and wait for `ASN0109I` in its log.
6. Because this fixture has no Apply program to send CAPSTART signals, run
   `UPDATE ASN.IBMSNAP_REGISTER SET STATE = 'A' WHERE SOURCE_OWNER = 'DB2INST1'`,
   then `asnccmd capture_server=TESTDB capture_schema=ASN reinit` and wait for
   `ASN0023I`. These administrator actions are confined to fixture setup.
7. Insert the seed rows and look up each registration's `CD_OWNER` and `CD_TABLE`
   in `ASN.IBMSNAP_REGISTER`. Wait until each CD table contains its sentinel row
   at or below the global `SYNCHPOINT`, then mark the service ready. CD tables
   need not be in the source table's schema.

The health check waits for initialization, loads the instance's `db2profile`,
and connects to `TESTDB`. It terminates its CLP session after each probe. If
setup fails, the test includes the container's health-check output in its diagnostics.

### Running tests

Go tests require IBM's clidriver headers and libraries. `run_tests.sh` uses an
already-installed driver at `$IBM_DB_HOME`, defaulting to
`$(go env GOPATH)/pkg/mod/github.com/ibmdb/clidriver`. The Dockerfile installs it
with the same installer as `source-db2-batch`.

The connector image and Go test process set `DB2CODEPAGE=1208` before driver
initialization so character results are UTF-8 regardless of the host locale.
The fixture also sets it inside the `su - db2inst1` session before starting the
Db2 command-line processor, so the UTF-8 seed literals are stored correctly.

```bash
# Unit tests; no database required.
source-db2/run_tests.sh

# Initialize a fresh fixture and run all live tests.
source tests/source-db2/setup.sh
TEST_DATABASE=yes TEST_DB2_PASSWORD=flowtest1 source-db2/run_tests.sh -run Integration \
  -db_capture_addr localhost:50000 -db_capture_user db2inst1 -db_name TESTDB
tests/source-db2/cleanup.sh

# Test the built image, including spec, discovery, and backfill handover.
./build-local.sh source-db2
CONNECTOR=source-db2 VERSION=local ./tests/run.sh
```

The repository end-to-end test recreates its own database, so it does not need
another fixture to be running. It captures four rows in chunks of two and
compares the complete documents, including types, nulls, Unicode DBCLOB
contents, preserved column names, and source metadata, with
`tests/source-db2/snapshot.jsonl`. The shared
runner needs `flowctl` and `jq`; setup provides a Python 3 fallback if `envsubst`
is unavailable. `TestIntegrationCDC` runs the real connector in-process against both dedicated
registration styles. It verifies backfill, insert, update, key change, delete,
a transaction across both tables, and checkpoint restart without loss or replay.
It clears both tables and waits for publication before starting. Discovery uses
the shared ORDERS/LINES fixture; LOB tests create and drop their own tables and
check CLOB, BLOB, Unicode DBCLOB, and XML values around the 65,535-byte boundaries,
including keyless backfills without a primary key or collection key.
CI installs the same clidriver as the Dockerfile and runs all `Integration` tests
against this fixture, with teardown on success or failure.
Unit tests cover merge ordering, update pairs across batches, key encoding,
registration safety, and deterministic watermark deadlines.
