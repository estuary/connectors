Flow PostgreSQL Source Connector
================================

This is an [Airbyte Specification](https://docs.airbyte.io/understanding-airbyte/airbyte-specification)
compatible connector that captures change events from a PostgreSQL database via
[Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html).

## Getting Started

Prebuild connector images should be available at `ghcr.io/estuary/source-postgres`. See
"Connector Development" for instructions to build locally.

This connector operates via Logical Replication, so the source PostgreSQL instance
will need to run with `wal_level=logical`, and will need to have a replication slot
and publication defined. Unless overridden in `config.json` the slot/publication names
will default to `flow_slot` and `flow_publication` respectively, and they can be
created using:

```bash
psql "${POSTGRES_URI}" -c "SELECT pg_create_logical_replication_slot('flow_slot', 'pgoutput');"
psql "${POSTGRES_URI}" -c "CREATE PUBLICATION flow_publication FOR ALL TABLES;"
```

A minimal `config.json` consists solely of the database connection URI. Refer to the
output of `docker run --rm -it ghcr.io/estuary/source-postgres spec` for a list of
other supported config options:

```json
{
  "connectionURI": "postgres://flow:flow@localhost:5432/flow"
}
```

## Mechanism of Operation

In the "happy path" of operation, the connector starts or resumes replication using
the configured replication slot and publication. Each `INSERT/UPDATE/DELETE` event
received from the database for a configured stream is emitted as an equivalent JSON
record message, and each `COMMIT` event causes a new state update to be emitted.

When a new stream is added to `catalog.json`, the connector will scan all preexisting
rows from the table and translate them into `INSERT` events before resuming replication.
This scanning process takes place in "chunks", and can be resumed across connector
restarts until finished, however a long-running table scan will block replication
event processing until complete. Once replication begins/resumes, any change event
whose effect was already observed by the initial table scan will be suppressed.

The initial table scan requires a "primary key" which will be used to divide up the
table into chunks. The primary key may span multiple columns. Normally the connector
will default to using the primary key of the underlying table, unless overridden via
the `primary_key` property in `catalog.json`. If the underlying table has no primary
key then it must be explicitly provided in the catalog.

## Connector Development

Any meaningful connector development will require a test database to run
against. To set this up, run the following commands:

```bash
cd source-postgres && docker-compose up
export POSTGRES_URI='postgres://flow:flow@localhost:5432/flow'
psql "${POSTGRES_URI}" -c "SELECT pg_create_logical_replication_slot('flow_slot', 'pgoutput');"
psql "${POSTGRES_URI}" -c "CREATE PUBLICATION flow_publication FOR ALL TABLES;"
```

The connector has a reasonably thorough `go test` suite which assumes the existence of
this test database. The easy way to build the connector and run those tests is via
`docker build`:

```bash
docker build --network=host -f source-postgres/Dockerfile -t ghcr.io/estuary/source-postgres:dev .
```

You can also run the resulting connector image manually:

```bash
docker run --rm -it --network=host -v <configsDir>:/cfg ghcr.io/estuary/source-postgres:dev read --config=/cfg/config.json --catalog=/cfg/catalog.json --state=/cfg/state.json
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
CONNECTOR=source-postgres VERSION=dev POSTGRES_CONNECTION_URI='postgres://flow:flow@localhost:5432/flow' ./tests/run.sh
```
