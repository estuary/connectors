Flow PostgreSQL Source Connector
================================

This is a Flow [capture connector](https://docs.estuary.dev/concepts/captures/)
which captures change events from a PostgreSQL database via
[Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html).

## Connector Development

The connector has a fairly comprehensive suite of automated tests which can be
run via `go test`. Most of them require an external database to interact with,
so typical usage looks something like:

```bash
## Start the test database running
$ docker compose -f source-postgres/docker-compose.yaml up -d

## Run the complete test suite against the database we just launched
$ TEST_DATABASE=yes go test -v ./source-postgres/ -count=1
```

The `go test` suite only exercises connector behavior in isolation. There's another
integration test which runs it as part of a Flow catalog as well:

```bash
## Build a local version of the connector image
$ docker build --network=host -t ghcr.io/estuary/source-postgres:local -f source-postgres/Dockerfile .

## Run the integration test
$ CONNECTOR=source-postgres VERSION=local ./tests/run.sh
```

## Connector Usage

Prebuilt connector images are available at [`ghcr.io/estuary/source-postgres`](ghcr.io/estuary/source-postgres).

The connector requires some amount of database setup in order to run:

* Logical replication must be enabled on the database ([`wal_level=logical`](https://www.postgresql.org/docs/current/runtime-config-wal.html)).

* The user account performing the capture must have appropriate permissions:
  - The [`REPLICATION`](https://www.postgresql.org/docs/current/sql-createrole.html) attribute is required to open a replication connection.
  - Permission to write to the watermarks table is required.
  - Permission to read the tables being captured is required.

* There must be a [publication](https://www.postgresql.org/docs/current/sql-createpublication.html) representing the set of tables for which change events should be reported.

  - Unless overridden in the capture config this should be named `"flow_publication"`.
  - If this doesn't exist the connector will attempt to create one, but this will
    typically fail unless it's running with superuser credentials.

* There must be a [replication slot](https://www.postgresql.org/docs/current/warm-standby.html#STREAMING-REPLICATION-SLOTS). The replication slot represents a "cursor" into
  the PostgreSQL write-ahead log from which change events can be read.

  - Unless overridden in the capture config this will be named `"flow_slot"`.
  - This will be created automatically by the connector if it doesn’t already exist.

* There must be a watermarks table. The watermarks table is a small "scratch space"
  to which the connector occasionally writes a small amount of data (a UUID,
  specifically) to ensure accuracy when backfilling preexisting table contents.

  - Unless overridden this will be named `"public.flow_watermarks"`.
  - This will be created automatically if the connector has suitable permissions,
    but must be created manually (see below) in more restricted setups.

The connector will attempt to create the replication slot, publication,
and watermarks table if necessary and they don't already exist, so one
way of satisfying all these requirements (other than `wal_level=logical`,
which must be done manually) is for the capture to connect as a database
superuser. A more restricted setup can be achieved with something like
the following example:

```sql
CREATE USER flow_capture WITH PASSWORD 'secret' REPLICATION;

-- The `pg_read_all_data` role is new in PostgreSQL v14. For older versions:
--
--   GRANT USAGE ON SCHEMA public, <others> TO flow_capture;
--   ALTER DEFAULT PRIVILEGES IN SCHEMA public, <others> GRANT SELECT ON TABLES to flow_capture;
--   GRANT SELECT ON ALL TABLES IN SCHEMA public, <others> TO flow_capture;
--
-- can be substituted, but all schemas which will be captured from must be listed.
GRANT pg_read_all_data TO flow_capture;

CREATE PUBLICATION flow_publication FOR ALL TABLES;
ALTER PUBLICATION flow_publication SET (publish_via_partition_root = true);

-- Set WAL level to logical. Note that changing this requires a database
-- restart to take effect.
ALTER SYSTEM SET wal_level = logical;
```

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

## Running Tests against Supabase PostgreSQL

The test database set up via `docker-compose.yaml` runs the script `init-user-db.sh` to
set up a capture user with all necessary permissions to execute the test suite. However
it can be annoying to have to juggle two distinct Postgres instances if you're running
the whole Flow stack locally and thus have Supabase running.

It is possible to run the test suite against the Supabase Postgres instance with a
little bit of manual setup (which, you may note, is exactly the same setup that is
performed automatically by `init-user-db.sh` if you use `docker-compose.yaml`):

    $ psql postgresql://postgres:postgres@localhost:5432/postgres
    > CREATE USER flow_capture WITH PASSWORD 'secret1234' REPLICATION;

    > CREATE SCHEMA IF NOT EXISTS test;
    > GRANT USAGE ON SCHEMA test TO flow_capture;
    > ALTER DEFAULT PRIVILEGES IN SCHEMA test GRANT SELECT ON TABLES to flow_capture;
    > GRANT SELECT ON ALL TABLES IN SCHEMA test TO flow_capture;

    > CREATE PUBLICATION flow_publication FOR ALL TABLES;
    > ALTER PUBLICATION flow_publication SET (publish_via_partition_root = true);