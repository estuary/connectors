Flow PostgreSQL Source Connector
================================

This is an [Airbyte Specification](https://docs.airbyte.io/understanding-airbyte/airbyte-specification)
compatible connector that captures change events from a PostgreSQL database via
[Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html).

## Getting Started

Prebuild connector images should be available at `ghcr.io/estuary/source-postgres`. See
"Connector Development" for instructions to build locally.

The connector has several prerequisites:
* Logical replication must be enabled on the database ([`wal_level=logical`](https://www.postgresql.org/docs/current/runtime-config-wal.html)).
* There must be a [replication slot](https://www.postgresql.org/docs/current/warm-standby.html#STREAMING-REPLICATION-SLOTS). The replication slot represents a "cursor" into
  the PostgreSQL write-ahead log from which change events can be read.
  - Unless overridden in the capture config this will be named `"flow_slot"`.
  - This will be created automatically by the connector if it doesn’t already exist.
* There must be a [publication](https://www.postgresql.org/docs/current/sql-createpublication.html). The publication represents the set of tables for which
  change events will be reported.
  - Unless overridden in the capture config this will be named `"flow_publication"`.
  - This will be created automatically if the connector has suitable permissions,
    but must be created manually (see below) in more restricted setups.
* There must be a watermarks table. The watermarks table is a small "scratch space"
  to which the connector occasionally writes a small amount of data (a UUID,
  specifically) to ensure accuracy when backfilling preexisting table contents.
  - Unless overridden this will be named `"public.flow_watermarks"`.
  - This will be created automatically if the connector has suitable permissions,
    but must be created manually (see below) in more restricted setups.
* The connector must connect to PostgreSQL with appropriate permissions:
  - The [`REPLICATION`](https://www.postgresql.org/docs/current/sql-createrole.html) attribute is required to open a replication connection.
  - Permission to write to the watermarks table is required.
  - Permission to read the tables being captured is required.
  - Permission to read tables from `information_schema` and `pg_catalog` schemas is required for automatic discovery.

The connector will attempt to create the replication slot, publication,
and watermarks table if necessary and they don't already exist, so one
way of satisfying all these requirements (other than `wal_level=logical`,
which must be done manually) is for the capture to connect as a database
superuser. A more restricted setup can be achieved with something like
the following example:

```sql
CREATE USER flow_capture WITH PASSWORD 'secret' REPLICATION;

# The `pg_read_all_data` role is new in PostgreSQL v14. For older versions:
#
#   GRANT SELECT ON ALL TABLES IN SCHEMA public, <others> TO flow_capture;
#   GRANT SELECT ON ALL TABLES IN SCHEMA information_schema, pg_catalog TO flow_capture;
#
# can be substituted, but all schemas which will be captured from must be listed.
#
# If an even more restricted set of permissions is desired, you can also grant SELECT on
# just the specific table(s) which should be captured from. The ‘information_schema’ and
# ‘pg_catalog’ access is required for stream auto-discovery, but not for capturing already
# configured streams.
GRANT pg_read_all_data TO flow_capture;

CREATE TABLE IF NOT EXISTS public.flow_watermarks (slot TEXT PRIMARY KEY, watermark TEXT);
GRANT ALL PRIVILEGES ON TABLE public.flow_watermarks TO flow_capture;

CREATE PUBLICATION flow_publication FOR ALL TABLES;

# Set WAL level to logical. Note that changing this requires a database
# restart to take effect.
ALTER SYSTEM SET wal_level = logical;
```

A minimal `config.json` consists solely of the database connection parameters:

```json
{
  "database": "flow",
  "host": "localhost",
  "password": "flow",
  "port": 5432,
  "user": "flow_capture"
}
```

Refer to the output of `docker run --rm -it ghcr.io/estuary/source-postgres spec` for
a list of other supported config options.

## Mechanism of Operation

The connector streams change events from the database via Logical Replication, but
in general there can be large amounts of preexisting data in those tables which
cannot be obtained via replication. So the connector must also issue `SELECT`
queries to "backfill" this preexisting data. Unfortunately, naively merging
replicated change events with backfilled data can, in various edge cases,
result in missed or duplicated changes. What we want is for our capture output
to start out with insertion events for each row, and then only report changes
occurring after the previous row state.

This is achieved by using **watermark writes** to establish a causal relationship
between backfill results and the replication event stream, **buffering** the most
recent chunk of backfill data for a period of time, and **patching** replication
events falling within that chunk into the buffered results. In pseudo-code, the
core loop of the capture looks like:

```
While there are still streams being backfilled:

 1. Write a new watermark UUID.
 2. Stream change events until that watermark write is observed,
    patching changes which fall within the buffered result set
    and ignoring changes which fall past the buffered results.
 3. Emit the buffered results from the last backfill query, and
    if any streams are now done backfilling, mark them as fully active.
 4. Issue another backfill query to read a new chunk of rows from
    all streams which are still being backfilled.

Stream change events until a final "shutdown" watermark is observed.
In production use this shutdown watermark doesn't exist, so the capture
will continue to run indefinitely until stopped.
```

## Connector Development

Any meaningful connector development will require a test database to run
against. To set this up, run:

```bash
docker-compose --file source-postgres/docker-compose.yaml up --detach postgres
```

The connector has a reasonably thorough `go test` suite which assumes the existence of
this test database. The easy way to build the connector and run those tests is via
`docker build`:

```bash
docker build --network=host -f source-postgres/Dockerfile -t ghcr.io/estuary/source-postgres:dev .
```

You can also run the resulting connector image manually:

```bash
docker run --rm -it --network=host -v <configsDir>:/cfg \
  ghcr.io/estuary/source-postgres:dev read \
  --config=/cfg/config.json \
  --catalog=/cfg/catalog.json \
  --state=/cfg/state.json
```

However the connector is written entirely in Go and can also be built/tested/run via
`go build` / `go test` / `go run`, so that might be an easier option for one-off
manual testing:

```bash
go run . read --config=config.json --catalog=catalog.json --state=state.json
```

## Flow End-to-End Integration Test

The `go test` suite only tests the connector in isolation. There's another test
which runs it as part of a Flow catalog:

```bash
PGDATABASE=flow \
PGHOST=localhost \
PGPASSWORD=flow \
PGPORT=5432 \
PGUSER=flow \
CONNECTOR=source-postgres \
VERSION=dev \
./tests/run.sh
```