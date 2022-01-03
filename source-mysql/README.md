Flow MySQL Source Connector
===========================

This is an [Airbyte Specification](https://docs.airbyte.io/understanding-airbyte/airbyte-specification)
compatible connector that captures change events from a MySQL database via the
[Binary Log](https://dev.mysql.com/doc/refman/8.0/en/binary-log.html).

## Getting Started

Prebuild connector images are available at `ghcr.io/estuary/source-mysql`. See
"Connector Development" for instructions to build locally.

The connector has several prerequisites:
* The [`binlog_row_metadata`](https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_row_metadata)
  system variable must be set to `FULL`.
  - Note that this can be done on a dedicated replica even if the primary database has it set to `MINIMAL`.
* There must be a watermarks table. The watermarks table is a small "scratch space"
  to which the connector occasionally writes a small amount of data (a UUID,
  specifically) to ensure accuracy when backfilling preexisting table contents.
  - By default this is named `"flow.watermarks"` but this can be overridden in `config.json`.
* The capture user must have appropriate permissions:
  - The `REPLICATION CLIENT` and `REPLICATION SLAVE` privileges.
  - Permission to insert/update/delete on the watermarks table.
  - Permission to read the tables being captured.
  - Permission to read from `information_schema` tables is required (if automatic discovery is used).

This can be achieved with the following example commands:

```sql
# Create the watermarks table. This table can have any name and be in any database,
# so long as config.json is modified accordingly.
CREATE DATABASE IF NOT EXISTS flow;
CREATE TABLE IF NOT EXISTS flow.watermarks (slot INTEGER PRIMARY KEY, watermark TEXT);

# Create the 'flow_capture' user with replication permission, the ability to
# read all tables, and the ability to read/write the watermarks table. The
# SELECT permission can be restricted to just the tables that need to be
# captured, but automatic discovery requires `information_schema` access too.
CREATE USER IF NOT EXISTS flow_capture
  IDENTIFIED BY 'secret'
  COMMENT 'User account for Flow MySQL data capture';
GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'flow_capture';
GRANT SELECT ON *.* TO 'flow_capture';
GRANT INSERT, UPDATE, DELETE ON flow.watermarks TO 'flow_capture';

SET PERSIST binlog_row_metadata = 'FULL';
```

This corresponds to the following example `config.json`:

```json
{
    "address": "127.0.0.1:3306",
    "user": "flow_capture",
    "pass": "secret",
    "dbname": "test",
    "server_id": 12345,
    "watermarks_table": "flow.watermarks"
}
```

Refer to the output of `docker run --rm -it ghcr.io/estuary/source-mysql spec` for
the full list of supported config options.

## Mechanism of Operation

See [`source-postgres/README.md`](https://github.com/estuary/connectors/blob/main/source-postgres/README.md#mechanism-of-operation) for this explanation.

## Connector Development

Any meaningful connector development will require a test database to run
against. To set this up, run:

```bash
docker-compose --file source-mysql/docker-compose.yaml up --detach
```

The connector has a reasonably thorough `go test` suite which assumes the existence of
this test database. The easy way to build the connector and run those tests is via
`docker build`:

```bash
docker build --network=host -f source-mysql/Dockerfile -t ghcr.io/estuary/source-mysql:dev .
```

You can also run the resulting connector image manually:

```bash
docker run --rm -it --network=host -v <configsDir>:/cfg \
  ghcr.io/estuary/source-mysql:dev read \
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
MYSQL_DATABASE=test \
MYSQL_HOST=127.0.0.1 \
MYSQL_PORT=3306 \
MYSQL_USER=root \
MYSQL_PWD=flow \
MYSQL_SERVERID=12345 \
CONNECTOR=source-mysql \
VERSION=dev \
./tests/run.sh
```